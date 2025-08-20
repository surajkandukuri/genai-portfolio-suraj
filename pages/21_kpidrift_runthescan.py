# pages/21_kpidrift_runthescan.py
from __future__ import annotations

import os, re, uuid, hashlib, traceback
import datetime as dt
from typing import List, Tuple

import streamlit as st
from supabase import create_client, Client

from provisioning.theme import page_header
from provisioning.ui import card

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="KPI Drift Hunter — Run the Scan", layout="wide")
page_header(
    "KPI Drift Hunter — Run the Scan",
    "Paste two BI report URLs → capture full-page + widget crops → store in Supabase."
)

# ── Config helpers ───────────────────────────────────────────────────────────
def sget(*keys, default=None):
    for k in keys:
        try:
            if k in st.secrets:
                return st.secrets[k]
        except Exception:
            pass
        v = os.getenv(k)
        if v:
            return v
    return default

SUPABASE_URL = sget("SUPABASE_URL", "SUPABASE__URL")
SUPABASE_KEY = sget("SUPABASE_SERVICE_ROLE_KEY","SUPABASE_ANON_KEY","SUPABASE_SERVICE_KEY","SUPABASE__SUPABASE_SERVICE_KEY")
KDH_BUCKET   = sget("KDH_BUCKET", default="kpidrifthunter")

@st.cache_resource
def get_sb() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        st.error("Missing Supabase config. Add SUPABASE_URL and a SERVICE_ROLE/ANON key in .streamlit/secrets.toml")
        st.stop()
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        st.error(f"Could not connect to Supabase: {e}")
        st.stop()

sb: Client = get_sb()

# Optional table existence check
try:
    sb.table("kdh_screengrab_dim").select("screengrab_id").limit(1).execute()
    sb.table("kdh_widget_dim").select("widget_id").limit(1).execute()
except Exception as e:
    st.error(f"DB check failed. Did you run the KPI Drift Hunter DDL? Error: {e}")
    st.stop()

# ── Utilities ────────────────────────────────────────────────────────────────
def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"https?://", "", text)
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text[:80] or "report"

def day_prefix(session_id: str, report_slug: str) -> str:
    now = dt.datetime.utcnow()
    return f"{KDH_BUCKET}/{now:%Y/%m/%d}/{session_id}/{report_slug}"

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def detect_platform(url: str, dom_html: str | None = None) -> Tuple[str, str, float]:
    u = url.lower()
    if "powerbi.com" in u: return ("powerbi", "url", 0.99)
    if "tableau.com" in u: return ("tableau", "url", 0.99)
    if dom_html:
        d = dom_html.lower()
        if "powerbi" in d: return ("powerbi", "dom", 0.80)
        if "tableau" in d: return ("tableau", "dom", 0.80)
    return ("unknown", "url", 0.10)

def _key_only(bucket: str, key: str) -> str:
    return "/".join(key.split("/")[1:]) if key.startswith(bucket + "/") else key

def storage_upload_bytes(bucket: str, key: str, data: bytes, content_type="image/png") -> None:
    sb.storage.from_(bucket).upload(_key_only(bucket, key), data, {"content-type": content_type, "upsert": True})

def storage_signed_url(bucket: str, key: str, ttl_sec=3600) -> str:
    return sb.storage.from_(bucket).create_signed_url(_key_only(bucket, key), ttl_sec)["signedURL"]

def insert_screengrab_row(row: dict) -> dict:
    try:
        res = sb.table("kdh_screengrab_dim").insert(row).execute()
        return res.data[0]
    except Exception:
        res = (sb.table("kdh_screengrab_dim")
               .select("*")
               .eq("screengrab_hashvalue", row["screengrab_hashvalue"])
               .limit(1).execute())
        return res.data[0] if res.data else row

def insert_widget_rows(rows: List[dict]) -> None:
    if rows:
        sb.table("kdh_widget_dim").insert(rows).execute()

def validate_public_url(u: str) -> None:
    lo = (u or "").lower()
    if "powerbi.com" in lo and "/view?" not in lo:
        raise ValueError("Power BI link is not a public embed. Use a URL like https://app.powerbi.com/view?r=...")

# ── Playwright capture ───────────────────────────────────────────────────────
def ensure_playwright_installed() -> None:
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
        return
    except Exception:
        os.system("python -m playwright install chromium")  # nosec
        os.system("python -m playwright install webkit")    # nosec
        from playwright.sync_api import sync_playwright  # noqa: F401

def _load_and_wait_tableau(page, url: str):
    """
    Robust loader for Tableau Public pages.
    - Waits for outer doc (domcontentloaded)
    - Waits for placeholder/iframe
    - Waits for canvases/SVG inside the iframe
    - Scrolls to force paint
    """
    from playwright.sync_api import TimeoutError as PWTimeout
    page.goto(url, wait_until="domcontentloaded", timeout=90000)

    # Wait for common Tableau markers on the top-level page
    try:
        page.wait_for_selector("iframe, .tableauPlaceholder, .tab-bootstrap", timeout=45000)
    except PWTimeout:
        # still try a short settle
        page.wait_for_timeout(3000)

    # Force layout/paint
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(800)
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(800)

    # Wait inside the Tableau iframe if present
    tableau_frames = [f for f in page.frames
                      if "tableau" in (f.url or "").lower()
                      or "public.tableau.com" in (f.url or "").lower()
                      or "/views/" in (f.url or "").lower()]
    if tableau_frames:
        f = tableau_frames[0]
        try:
            f.wait_for_selector("canvas, svg, .tab-content, .tableauPlaceholder", timeout=45000)
        except PWTimeout:
            page.wait_for_timeout(3000)

def _browser_try(p, engine: str, url: str, session_id: str, prefix: str, capture_charts: bool):
    """
    One attempt with a specific browser engine.
    Returns (sg_row, widgets, full_signed)
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    launch_args = ["--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu"]
    if engine == "chromium":
        browser = p.chromium.launch(args=launch_args, headless=True)
    elif engine == "webkit":
        browser = p.webkit.launch(headless=True)
    else:
        raise ValueError("Unsupported engine: " + engine)

    ctx = browser.new_context(
        viewport={"width": 1600, "height": 1400},
        device_scale_factor=1.25,
        locale="en-US",
        user_agent="Mozilla/5.0 (KPIDriftHunter)"
    )
    page = ctx.new_page()
    page.set_default_timeout(90000)

    # Load with Tableau‑robust routine by default (safe for others too)
    _load_and_wait_tableau(page, url)

    # DOM + full screenshot
    dom_html: str = page.content()
    dom_hash = sha256_bytes(dom_html.encode("utf-8"))
    png = page.screenshot(full_page=True, type="png")
    full_key = f"{prefix}/full.png"
    storage_upload_bytes(KDH_BUCKET, full_key, png)
    full_signed = storage_signed_url(KDH_BUCKET, full_key)

    platform, detected_via, confidence = detect_platform(url, dom_html)
    now = dt.datetime.utcnow()
    sg_row = {
        "screengrab_id": str(uuid.uuid4()),
        "capture_session_id": session_id,
        "url": url,
        "platform": platform,
        "detected_via": detected_via,
        "platform_confidence": round(confidence, 3),
        "screengrab_hashvalue": dom_hash,
        "storage_bucket": KDH_BUCKET,
        "storage_path_full": full_key,
        "captured_at": now.isoformat(),
        "rec_eff_strt_dt": now.isoformat(),
        "curr_rec_ind": True,
    }
    sg_db = insert_screengrab_row(sg_row)

    # Widget crops (prefer inside frames for Tableau)
    widgets: List[dict] = []
    if capture_charts:
        candidates = []
        # top-level
        candidates.extend(page.query_selector_all("canvas, svg, [role='img'], .visual, .chart"))
        # frames
        for fr in page.frames:
            try:
                candidates.extend(fr.query_selector_all("canvas, svg, [role='img'], .tab-content"))
            except Exception:
                pass

        seen = 0
        for el in candidates:
            try:
                box = el.bounding_box()
                if not box or box["width"] < 80 or box["height"] < 80:
                    continue
                w_png = el.screenshot(type="png")
                w_key = f"{prefix}/widgets/w_{seen}.png"
                storage_upload_bytes(KDH_BUCKET, w_key, w_png)
                widgets.append({
                    "widget_id": str(uuid.uuid4()),
                    "screengrab_id": sg_db["screengrab_id"],
                    "bbox_xywh": [int(box["x"]), int(box["y"]), int(box["width"]), int(box["height"])],
                    "storage_bucket": KDH_BUCKET,
                    "storage_path_crop": w_key,
                    "extraction_stage": "captured",
                    "insrt_dttm": now.isoformat(),
                    "rec_eff_strt_dt": now.isoformat(),
                    "curr_rec_ind": True,
                    "parsed_to_fact": False,
                })
                seen += 1
                if seen >= 24:   # keep it reasonable
                    break
            except Exception:
                continue

    insert_widget_rows(widgets)
    ctx.close()
    browser.close()
    return sg_db, widgets, full_signed

def capture_url(url: str, session_id: str, capture_charts: bool = True) -> dict:
    """
    Returns: {'screengrab': {...}, 'widgets': [...], 'thumb_url': <signed url>}
    Retries on WebKit if Chromium fails (helps for some Tableau renders).
    """
    ensure_playwright_installed()
    from playwright.sync_api import sync_playwright

    validate_public_url(url)

    report_slug = slugify(url)
    prefix = day_prefix(session_id, report_slug)

    with sync_playwright() as p:
        last_err = None
        for engine in ("chromium", "webkit"):
            try:
                sg_db, widgets, full_signed = _browser_try(p, engine, url, session_id, prefix, capture_charts)
                return {"screengrab": sg_db, "widgets": widgets, "thumb_url": full_signed}
            except Exception as e:
                last_err = f"[{engine}] {e}\n{traceback.format_exc()}"
                # try next engine
        # If both engines fail, raise with full trace so Streamlit shows details
        raise RuntimeError(f"All capture attempts failed.\n{last_err}")

# ── UI ───────────────────────────────────────────────────────────────────────
with card("Paste two BI report URLs"):
    with st.form("kdh_form"):
        c1, c2 = st.columns([1, 1])
        url_a = c1.text_input("Report A URL", placeholder="https://... (Power BI or Tableau)")
        url_b = c2.text_input("Report B URL", placeholder="https://... (Power BI or Tableau)")
        run = st.form_submit_button("Run Screen‑Grab", use_container_width=True)

if run:
    urls = [u.strip() for u in [url_a, url_b] if u and u.strip()]
    if not urls:
        st.warning("Please paste at least one URL.")
        st.stop()

    session_id = str(uuid.uuid4())
    st.info(f"Capture session: `{session_id}`")
    results = []

    for idx, u in enumerate(urls, start=1):
        with st.spinner(f"Capturing report {idx}…"):
            try:
                res = capture_url(u, session_id, capture_charts=True)
                results.append(res)
            except Exception as e:
                # Show full reason (engine traces etc.)
                st.error(f"Failed to capture {u}\n\n{e}")

    if results:
        st.success("Capture complete. Stored in Supabase and recorded in DB.")
        for i, r in enumerate(results, start=1):
            sg = r["screengrab"]
            with st.container(border=True):
                st.subheader(f"Report {i}")
                c1, c2 = st.columns([1, 2], vertical_alignment="top")
                with c1:
                    st.image(r["thumb_url"], caption="Full-page screengrab", use_container_width=True)
                    st.caption("Signed link (expires):")
                    st.link_button("Open full image", r["thumb_url"], use_container_width=True)
                with c2:
                    st.markdown(
                        f"""
**URL:** {sg['url']}  
**Platform:** `{sg['platform']}` (via {sg['detected_via']}, conf={sg['platform_confidence']})  
**Stored at:** `{sg['storage_path_full']}`  
**Captured at:** {sg['captured_at']} UTC  
**Widgets captured:** {len(r['widgets'])}
                        """.strip()
                    )
                    if r["widgets"]:
                        with st.expander("Widget crops"):
                            rows = []
                            for w in r["widgets"]:
                                signed = storage_signed_url(w["storage_bucket"], w["storage_path_crop"], 3600)
                                rows.append(
                                    f"- `{w['bbox_xywh']}` · stage=`{w['extraction_stage']}` · "
                                    f"[open]({signed}) · `{w['storage_path_crop']}`"
                                )
                            st.markdown("\n".join(rows))

st.caption("Tip: Tableau — use Share links (…/views/...&publish=yes). Power BI — use Publish‑to‑web links (https://app.powerbi.com/view?r=...).")
