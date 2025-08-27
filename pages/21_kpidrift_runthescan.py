# pages/21_kpidrift_runthescan.py
from __future__ import annotations

# --- Windows asyncio subprocess fix (must be before Playwright is used) ---
import sys, asyncio
import platform
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import os, re, uuid, hashlib, traceback
import datetime as dt
from pathlib import Path
from typing import List, Tuple

import streamlit as st
from supabase import create_client, Client

from provisioning.theme import page_header
from provisioning.ui import card

# Provider-specific capture functions
from provisioning.a2_kpidrift_capture.a2_kpidrift_powerbi import capture_powerbi
from provisioning.a2_kpidrift_capture.a2_kpidrift_tableau import capture_tableau

# Persistence helpers (upload -> DB)
from provisioning.a2_kpidrift_capture.a2_kpidrift_persist import (
    upsert_screengrab,
    insert_widgets,
    image_wh,
)

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

def _key_only(bucket: str, key: str) -> str:
    return "/".join(key.split("/")[1:]) if key.startswith(bucket + "/") else key

def storage_upload_bytes(bucket: str, key: str, data: bytes, content_type="image/png") -> None:
    # Supabase Storage expects strings for header-backed options.
    # 'upsert' must be "true" / "false" (strings), not booleans.
    options = {
        "contentType": content_type,  # preferred casing in supabase-py v2
        "upsert": "true",
    }
    sb.storage.from_(bucket).upload(_key_only(bucket, key), data, options)


def storage_signed_url(bucket: str, key: str, ttl_sec=3600) -> str:
    return sb.storage.from_(bucket).create_signed_url(_key_only(bucket, key), ttl_sec)["signedURL"]

def validate_public_url(u: str) -> None:
    lo = (u or "").lower()
    if "powerbi.com" in lo and "/view?" not in lo:
        raise ValueError("Power BI link is not a public embed. Use a URL like https://app.powerbi.com/view?r=...")

# ── Playwright bootstrap (safety net) ────────────────────────────────────────
def ensure_playwright_installed() -> None:
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
        return
    except Exception:
        os.system("python -m playwright install chromium")  # nosec
        from playwright.sync_api import sync_playwright  # noqa: F401

# ── Capture adapter (upload → DB) ────────────────────────────────────────────
def capture_url(url: str, session_id: str) -> dict:
    """
    1) Capture locally via provider
    2) Upload images to Supabase storage
    3) Insert screengrab row, then widget rows
    Returns: {'screengrab': {...}, 'widgets': [...], 'thumb_url': <signed url>}
    """
    ensure_playwright_installed()
    validate_public_url(url)

    # 1) provider capture (local files)
    outdir = Path("screenshots")
    outdir.mkdir(parents=True, exist_ok=True)

    provider = "powerbi" if "powerbi.com" in url.lower() else ("tableau" if "tableau.com" in url.lower() else "unknown")
    if provider == "powerbi":
        result = capture_powerbi(url, outdir)
    elif provider == "tableau":
        result = capture_tableau(url, outdir)
    else:
        # default to tableau strategy for other public viz hosts
        result = capture_tableau(url, outdir)

    if not result.artifacts.full.exists():
        raise RuntimeError("Capture produced no full image.")

    # 2) upload to storage first
    report_slug = slugify(url)
    prefix = day_prefix(session_id, report_slug)

    full_png = result.artifacts.full.read_bytes()
    full_key = f"{prefix}/full.png"
    storage_upload_bytes(KDH_BUCKET, full_key, full_png)
    full_signed = storage_signed_url(KDH_BUCKET, full_key)

    crops_for_db: List[dict] = []
    if result.artifacts.report and result.artifacts.report.exists():
        crop_png = result.artifacts.report.read_bytes()
        crop_key = f"{prefix}/widgets/report_crop.png"
        storage_upload_bytes(KDH_BUCKET, crop_key, crop_png)
        w, h = image_wh(crop_png)  # derive bbox if no DOM coords
        crops_for_db.append({"bytes": crop_png, "path": crop_key, "bbox": [0, 0, w, h]})

    # 3) insert into DB (screengrab first, then widgets)
    sg_db = upsert_screengrab(
        sb,
        session_id=session_id,
        url=url,
        platform=provider,
        full_png_bytes=full_png,
        storage_bucket=KDH_BUCKET,
        storage_path_full=full_key,
        user_id=None,
    )

    if crops_for_db:
        insert_widgets(
            sb,
            screengrab_id=sg_db["screengrab_id"],
            storage_bucket=KDH_BUCKET,
            crops=crops_for_db,
        )

    return {"screengrab": sg_db, "widgets": crops_for_db, "thumb_url": full_signed}

# ── UI ───────────────────────────────────────────────────────────────────────
with card("Paste two BI report URLs"):
    c1, c2 = st.columns([1, 1])
    url_a = c1.text_input("Report A URL", placeholder="https://... (Power BI or Tableau)")
    url_b = c2.text_input("Report B URL", placeholder="https://... (Power BI or Tableau)")
    b1, b2, _ = st.columns([1,1,4])
    run_a = b1.button("Capture A", use_container_width=True, type="primary")
    run_b = b2.button("Capture B", use_container_width=True)

# use a single session id for a page run
session_id = st.session_state.get("kdh_session") or str(uuid.uuid4())
st.session_state["kdh_session"] = session_id

if run_a or run_b:
    urls = []
    if run_a and url_a.strip():
        urls.append(url_a.strip())
    if run_b and url_b.strip():
        urls.append(url_b.strip())

    if not urls:
        st.warning("Please paste a valid URL for the button you clicked.")
    else:
        st.info(f"Capture session: `{session_id}`")
        for idx, u in enumerate(urls, start=1):
            with st.spinner(f"Capturing report {idx}…"):
                try:
                    res = capture_url(u, session_id)
                    sg = res["screengrab"]
                    with st.container(border=True):
                        st.subheader(f"Report {idx}")
                        c1, c2 = st.columns([1, 2], vertical_alignment="top")
                        with c1:
                            st.image(res["thumb_url"], caption="Full-page screengrab", use_container_width=True)
                            st.caption("Signed link (expires):")
                            st.link_button("Open full image", res["thumb_url"], use_container_width=True)
                        with c2:
                            st.markdown(
                                f"""
**URL:** {sg['url']}  
**Platform:** `{sg['platform']}` (via url)  
**Stored at:** `{sg['storage_path_full']}`  
**Captured at:** {sg['captured_at']} UTC  
**Widgets captured:** {len(res['widgets'])}
                                """.strip()
                            )
                except Exception as e:
                    # Show full traceback to pinpoint where headers were built
                    tb = traceback.format_exc()
                    st.error(f"Failed to capture {u}\n\n{e}\n\n```traceback\n{tb}\n```")

st.caption("Tip: Tableau — use Share links (…/views/...&publish=yes). Power BI — use Publish-to-web links (https://app.powerbi.com/view?r=...).")
