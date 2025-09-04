# pages/22_kpidrift_widgetextractor.py
from __future__ import annotations

# ---- Windows asyncio fix (must be before Playwright is used) ----
import platform, asyncio
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import os, re, io, hashlib, traceback, time
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import streamlit as st
import pandas as pd
import requests
from supabase import create_client, Client
from PIL import Image, ImageDraw

# Reuse engine helpers
from provisioning.a2_kpidrift_capture.a2_kpidrift_engine import with_browser, ensure_outdir, nowstamp
from provisioning.a2_kpidrift_capture.a2_kpidrift_persist import insert_widgets, image_wh
from provisioning.a2_kpidrift_capture.a2_kpidrift_types import Artifacts, CaptureResult

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="KPI Drift — Widget Extractor", page_icon="🧩", layout="wide")
st.title("🧩 KPI Drift — Widget Extractor")
st.caption("Pick one or more screengrabs (full.png) and extract per-chart widgets with animated progress.")

# ── Config helpers ────────────────────────────────────────────────────────────
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

# ── Storage helpers ───────────────────────────────────────────────────────────
def _key_only(bucket: str, key: str) -> str:
    return "/".join(key.split("/")[1:]) if key.startswith(bucket + "/") else key

def storage_signed_url(bucket: str, key: str, ttl_sec=3600) -> str:
    return sb.storage.from_(bucket).create_signed_url(_key_only(bucket, key), ttl_sec)["signedURL"]

def storage_upload_bytes(bucket: str, key: str, data: bytes, content_type="image/png") -> None:
    # NOTE: 'upsert' must be STRING ("true"/"false")
    options = {"contentType": content_type, "upsert": "true"}
    sb.storage.from_(bucket).upload(_key_only(bucket, key), data, options)

# ── Load recent screengrabs from DB ───────────────────────────────────────────
@st.cache_data(ttl=60)
def load_recent_screengrabs(limit=200):
    q = (
        sb.table("kdh_screengrab_dim")
          .select("screengrab_id, url, platform, storage_path_full, captured_at")
          .order("captured_at", desc=True)
          .limit(limit)
          .execute()
    )
    rows = q.data or []
    for r in rows:
        try:
            r["thumb_url"] = storage_signed_url(KDH_BUCKET, r["storage_path_full"], ttl_sec=3600)
        except Exception:
            r["thumb_url"] = ""
    return rows

rows = load_recent_screengrabs()

if not rows:
    st.info("No screengrabs found yet. Run the scan first on page 21.")
    st.stop()

# Build a selection table using st.data_editor
df = pd.DataFrame(rows)
df["Select"] = False

st.subheader("Recent Screengrabs")
edited = st.data_editor(
    df[["Select", "thumb_url", "url", "platform", "captured_at", "screengrab_id", "storage_path_full"]],
    column_config={
        "Select": st.column_config.CheckboxColumn("Select"),
        "thumb_url": st.column_config.ImageColumn("Preview", help="Signed URL preview"),
        "url": st.column_config.LinkColumn("URL"),
        "platform": "Platform",
        "captured_at": "Captured At (UTC)",
        "screengrab_id": st.column_config.TextColumn("ID", help="DB screengrab id"),
        "storage_path_full": st.column_config.TextColumn("Storage Path"),
    },
    use_container_width=True,
    hide_index=True,
    num_rows="dynamic",
    height=420,
)

selected = edited[edited["Select"] == True]  # noqa: E712

st.divider()
c1, c2, _ = st.columns([1,1,6])
extract_btn = c1.button("▶️ Extract", type="primary", use_container_width=True, disabled=selected.empty)
refresh_btn = c2.button("↻ Refresh", use_container_width=True)

if refresh_btn:
    st.cache_data.clear()
    st.rerun()

# ── Visual helpers ───────────────────────────────────────────────────────────
def overlay_boxes_on_image(img_bytes: bytes, boxes: List[Tuple[int,int,int,int]]) -> bytes:
    im = Image.open(io.BytesIO(img_bytes)).convert("RGBA")
    draw = ImageDraw.Draw(im, "RGBA")
    for (x, y, w, h) in boxes:
        for off in range(2):
            draw.rectangle([x-off, y-off, x+w+off, y+h+off], outline=(0, 0, 0, 255))
    out = io.BytesIO()
    im.save(out, format="PNG")
    return out.getvalue()

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

# ── Minimal extractor (provider-agnostic with Power BI focus) ─────────────────
@with_browser(headless=True, scale=2.0, viewport=(1920, 1080))
def extract_widgets(ctx, url: str, screengrab_id: str, full_key: str, session_prefix: str) -> Dict:
    """
    Returns:
      {
        "widget_count": int,
        "crops": [{"bytes": ..., "path": ..., "bbox": [x,y,w,h], "label": "..."}],
        "overlays_key": "<path/to/overlays.png>",
      }
    """
    page = ctx.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(1200)

    # Find primary iframe (Power BI/Tableau embeds). We'll query inside it,
    # BUT element.bounding_box() is already in main-page coords. Do NOT add iframe offset.
    main_frame = page
    iframes = page.locator("iframe")
    if iframes.count() > 0:
        try:
            fr = iframes.first
            fr.wait_for(timeout=15_000)
            cf = fr.content_frame()
            if cf:  # use the content frame for querying
                main_frame = cf
        except Exception:
            pass

    # Do a quick inventory scroll to trigger lazy visuals (inside frame if present)
    try:
        scrollable = main_frame.evaluate("() => ({ h: document.documentElement.scrollHeight, vh: window.innerHeight })")
        total_h = int(scrollable.get("h", 0) or 0)
        vh = int(scrollable.get("vh", 0) or 800)
        steps = [0]
        # step ~70% viewport height up to 3 passes
        pos = vh
        while pos < total_h and len(steps) < 4:
            steps.append(pos)
            pos += int(vh * 0.7)
        for s in steps:
            main_frame.evaluate(f"window.scrollTo(0, {s});")
            page.wait_for_timeout(600)
    except Exception:
        pass  # harmless

    # Selector priority: containers → a11y roles → drawing surfaces
    SELECTOR_GROUPS = [
        # Power BI containers (capture whole visual incl. title)
        (".visualContainer, .visualContainerHost, .modernVisualOverlay", "container"),
        # A11y wrappers
        ("[role='figure'], [role='img']", "role"),
        # Tableau containers
        (".tab-worksheet, .tab-viz, .tabCanvas", "tableau"),
        # Drawing primitives (fallback)
        ("svg, canvas", "primitive"),
    ]

    MIN_W, MIN_H = 150, 100    # less conservative so slim charts aren't skipped
    PAD = 12                   # pad crops to include titles/axes

    candidates: List[Tuple[str, str, Tuple[int,int,int,int]]] = []  # (selector, kind, box)

    for sel, kind in SELECTOR_GROUPS:
        try:
            loc = main_frame.locator(sel)
            count = loc.count()
            n = min(20, count)  # cap per selector
            for i in range(n):
                el = loc.nth(i)
                try:
                    bb = el.bounding_box()
                    if not bb:
                        continue
                    x, y = int(bb["x"]), int(bb["y"])
                    w, h = int(bb["width"]), int(bb["height"])
                    if w < MIN_W or h < MIN_H:
                        continue
                    candidates.append((sel, kind, (x, y, w, h)))
                except Exception:
                    continue
        except Exception:
            continue

    # Deduplicate by IoU, but allow parent+child if kinds differ (e.g., container vs canvas)
    def iou(a, b) -> float:
        ax, ay, aw, ah = a
        bx, by, bw, bh = b
        x1, y1 = max(ax, bx), max(ay, by)
        x2, y2 = min(ax+aw, bx+bw), min(ay+ah, by+bh)
        inter = max(0, x2-x1) * max(0, y2-y1)
        if inter == 0:
            return 0.0
        ua = aw*ah + bw*bh - inter
        return inter / max(1, ua)

    kept: List[Tuple[str, str, Tuple[int,int,int,int]]] = []
    for sel, kind, box in sorted(candidates, key=lambda c: (c[2][1], c[2][0])):  # top→bottom, left→right
        drop = False
        for _, k_kind, k_box in kept:
            overlap = iou(box, k_box)
            # If heavy overlap and same "kind", drop; if kinds differ, allow (keeps container + canvas)
            if overlap > 0.7 and k_kind == kind:
                drop = True
                break
        if not drop:
            kept.append((sel, kind, box))

    # Download full.png (for overlay + PIL crops)
    full_signed = storage_signed_url(KDH_BUCKET, full_key, ttl_sec=3600)
    full_img_bytes = requests.get(full_signed, timeout=20).content
    full_im = Image.open(io.BytesIO(full_img_bytes)).convert("RGBA")
    W, H = full_im.size

    # Persistent live log instead of collapsing st.status
    log = st.container()
    prog = st.progress(0, text="Scanning widgets…")
    overlay_boxes: List[Tuple[int,int,int,int]] = []
    crops_for_db: List[Dict] = []

    total = max(1, len(kept))
    log.write(f"Found **{len(kept)}** candidates after de-dup.")

    # Helper to pad & clamp boxes
    def pad_clamp(x, y, w, h, pad=PAD):
        x2, y2 = x + w, y + h
        x_p = max(0, x - pad)
        y_p = max(0, y - pad)
        x2_p = min(W, x2 + pad)
        y2_p = min(H, y2 + pad)
        return x_p, y_p, x2_p - x_p, y2_p - y_p

    # Optional: find a nearby title above the box (Power BI)
    def find_title_text(box: Tuple[int,int,int,int]) -> Optional[str]:
        try:
            # Query elements likely to contain titles; filter by vertical proximity
            title_nodes = main_frame.locator(".visualTitle, [role='heading'], h1, h2, h3, h4, h5, h6")
            count = min(10, title_nodes.count())
            bx, by, bw, bh = box
            mid_x = bx + bw // 2
            closest = None
            best_dy = 99999
            for i in range(count):
                t = title_nodes.nth(i)
                tb = t.bounding_box()
                if not tb:
                    continue
                tx, ty, tw, th = int(tb["x"]), int(tb["y"]), int(tb["width"]), int(tb["height"])
                # vertically above and horizontally overlapping somewhat
                if ty < by and (tx < (bx + bw) and (tx + tw) > bx):
                    dy = by - ty
                    if dy < 160 and dy < best_dy:
                        txt = t.inner_text() or ""
                        txt = txt.strip()
                        if txt:
                            best_dy = dy
                            closest = txt
            return closest
        except Exception:
            return None

    for idx, (sel, kind, (x, y, w, h)) in enumerate(kept, start=1):
        # Pad box to include titles/axes
        px, py, pw, ph = pad_clamp(x, y, w, h, pad=PAD)
        overlay_boxes.append((px, py, pw, ph))
        # Update animated overlay preview (keeps log visible)
        if idx == 1 or idx % 2 == 0:
            st.image(overlay_boxes_on_image(full_img_bytes, overlay_boxes),
                     caption=f"Overlay after {idx} crops",
                     use_container_width=True)

        # Crop via PIL (stable & fast)
        crop_im = full_im.crop((px, py, px+pw, py+ph))
        bio = io.BytesIO()
        crop_im.save(bio, format="PNG")
        crop_bytes = bio.getvalue()

        # Try to capture a nearby title
        label = find_title_text((x, y, w, h)) or ""

        png_path = f"{session_prefix}/widgets/widget_{idx:02d}.png"
        storage_upload_bytes(KDH_BUCKET, png_path, crop_bytes, content_type="image/png")
        crops_for_db.append({"bytes": crop_bytes, "path": png_path, "bbox": [px, py, pw, ph], "label": label})

        log.write(f"✅ {idx}/{total} — kept `{kind}` from `{sel}` ({w}×{h})"
                  + (f" — **{label}**" if label else ""))

        prog.progress(min(idx/total, 1.0))

    # Save overlays.png
    overlays_key = f"{session_prefix}/widgets/overlays.png"
    storage_upload_bytes(KDH_BUCKET, overlays_key, overlay_boxes_on_image(full_img_bytes, overlay_boxes))

    return {
        "widget_count": len(crops_for_db),
        "crops": crops_for_db,
        "overlays_key": overlays_key,
    }

# ── Handle Extract action ─────────────────────────────────────────────────────
if extract_btn:
    if selected.empty:
        st.warning("Select at least one screengrab row to extract.")
        st.stop()

    for _, row in selected.iterrows():
        with st.container(border=True):
            st.subheader(f"Extracting: {row['platform']} — {row['url']}")
            try:
                full_key = row["storage_path_full"]  # e.g., .../<session>/<slug>/full.png
                base_prefix = "/".join(full_key.split("/")[:-1])  # drop 'full.png'
                session_prefix = base_prefix

                res = extract_widgets(
                    url=row["url"],
                    screengrab_id=row["screengrab_id"],
                    full_key=full_key,
                    session_prefix=session_prefix
                )

                # Persist widget rows
                if res["crops"]:
                    insert_widgets(
                        sb,
                        screengrab_id=row["screengrab_id"],
                        storage_bucket=KDH_BUCKET,
                        crops=[{k: v for k, v in c.items() if k in {"bytes","path","bbox"}}],  # DB helper expects these keys
                    )
                st.success(f"Extracted {res['widget_count']} widget(s).")
                st.link_button("Open overlays.png",
                               storage_signed_url(KDH_BUCKET, res["overlays_key"]),
                               use_container_width=False)

            except Exception as e:
                tb = traceback.format_exc()
                st.error(f"Failed to extract for {row['url']}\n\n{e}\n\n```traceback\n{tb}\n```")

    st.toast("Done! Refreshing list…", icon="✅")
    time.sleep(1.2)
    st.cache_data.clear()
    st.rerun()
