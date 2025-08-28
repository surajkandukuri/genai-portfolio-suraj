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
    # IMPORTANT: 'upsert' must be a STRING ("true"/"false"), not bool, as the client maps it into HTTP headers.
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
    # Add signed URLs for thumbnail display
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
df["Select"] = False  # checkbox column

# Column config to show images nicely
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
c1, c2, c3 = st.columns([1,1,6])
extract_btn = c1.button("▶️ Extract", type="primary", use_container_width=True, disabled=selected.empty)
refresh_btn = c2.button("↻ Refresh", use_container_width=True)

if refresh_btn:
    st.cache_data.clear()
    st.rerun()

# ── Animated extraction UX helpers ───────────────────────────────────────────
def overlay_boxes_on_image(img_bytes: bytes, boxes: List[Tuple[int,int,int,int]]) -> bytes:
    """Draw rectangle outlines on an image; return PNG bytes."""
    im = Image.open(io.BytesIO(img_bytes)).convert("RGBA")
    draw = ImageDraw.Draw(im, "RGBA")
    for (x, y, w, h) in boxes:
        # Outline rectangle; 3px width
        for off in range(3):
            draw.rectangle([x-off, y-off, x+w+off, y+h+off], outline=(0, 0, 0, 255))
    out = io.BytesIO()
    im.save(out, format="PNG")
    return out.getvalue()

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

# ── Minimal extractor (provider-agnostic heuristics) ─────────────────────────
# NOTE: This is a pragmatic extractor good enough for public Power BI/Tableau embeds.
# It captures multiple likely chart areas inside the first visible <iframe>.

@with_browser(headless=True, scale=2.0, viewport=(1920, 1080))
def extract_widgets(ctx, url: str, screengrab_id: str, full_key: str, session_prefix: str) -> Dict:
    """
    Returns a dict with:
    {
      "widget_count": int,
      "crops": List[{"bytes": ..., "path": ..., "bbox": [x,y,w,h]}],
      "overlays_key": "<path to overlays.png>",
    }
    """
    page = ctx.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(1500)

    # Find primary iframe
    iframes = page.locator("iframe")
    if iframes.count() == 0:
        # Fallback: treat top page as the visual container
        frame = page
        frame_offset = (0, 0)
    else:
        fr = iframes.first
        fr.wait_for(timeout=15_000)
        box = fr.bounding_box() or {"x":0,"y":0}
        frame_offset = (int(box["x"]), int(box["y"]))
        frame = fr.content_frame()
        if frame is None:
            # Try a moment later
            page.wait_for_timeout(1000)
            frame = fr.content_frame()
            if frame is None:
                frame = page
                frame_offset = (0, 0)

    # Candidate selectors across BI tools
    CANDIDATES = [
        "canvas", "svg", "[role='img']", "[role='figure']",
        ".visualContainer", ".visualContainerHost", ".modernVisualOverlay",
        ".tab-worksheet", ".tab-viz", ".tabCanvas", ".chart", ".highcharts-container"
    ]

    found = []
    for sel in CANDIDATES:
        try:
            loc = frame.locator(sel)
            n = min(12, loc.count())  # limit to avoid over-cropping
            for i in range(n):
                el = loc.nth(i)
                try:
                    bb = el.bounding_box()
                    if not bb:
                        continue
                    x, y, w, h = int(bb["x"]), int(bb["y"]), int(bb["width"]), int(bb["height"])
                    if w < 220 or h < 160:
                        continue
                    # Store absolute coords (page level) by adding iframe offset
                    abs_box = (x + frame_offset[0], y + frame_offset[1], w, h)
                    found.append((sel, abs_box))
                except Exception:
                    continue
        except Exception:
            continue

    # Deduplicate by overlap (simple IoU) and keep outermost-ish
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

    kept: List[Tuple[str, Tuple[int,int,int,int]]] = []
    for sel, box in found:
        if any(iou(box, kb) > 0.7 for _, kb in kept):
            continue
        kept.append((sel, box))

    # Sort top->bottom for nicer UX
    kept.sort(key=lambda kv: (kv[1][1], kv[1][0]))

    # Download full.png for overlay previews
    full_signed = storage_signed_url(KDH_BUCKET, full_key, ttl_sec=3600)
    import requests  # allowed in Streamlit runtime
    full_img_bytes = requests.get(full_signed, timeout=20).content

    crops_for_db: List[Dict] = []
    overlay_boxes: List[Tuple[int,int,int,int]] = []

    # Clip/screenshot from the live page for sharper crops
    prog = st.progress(0, text="Scanning widgets…")
    step = 0
    total = max(1, len(kept))

    with st.status("Extracting widgets…", expanded=True) as status:
        st.write(f"Found **{len(kept)}** candidates. Cropping and saving…")

        for idx, (sel, (x, y, w, h)) in enumerate(kept, start=1):
            # Animate: update progress + overlay
            overlay_boxes.append((x, y, w, h))
            overlay_img = overlay_boxes_on_image(full_img_bytes, overlay_boxes)
            st.image(overlay_img, caption=f"Overlay after {idx} crops", use_container_width=True)

            # Do the actual crop via page.screenshot(clip=…)
            try:
                clip = {"x": x, "y": y, "width": w, "height": h}
                png_path = f"{session_prefix}/widgets/widget_{idx:02d}.png"
                # Take a crisp screenshot directly from the page
                page.screenshot(path=None, clip=clip)
                # Playwright can't return bytes directly here, so re-screenshot area via DOM element when possible
                # Fallback to PIL crop from full image:
                im = Image.open(io.BytesIO(full_img_bytes)).convert("RGBA")
                crop_im = im.crop((x, y, x+w, y+h))
                out = io.BytesIO()
                crop_im.save(out, format="PNG")
                crop_bytes = out.getvalue()

                # Upload + stage for DB
                storage_upload_bytes(KDH_BUCKET, png_path, crop_bytes, content_type="image/png")
                crops_for_db.append({"bytes": crop_bytes, "path": png_path, "bbox": [x, y, w, h]})
                st.write(f"✅ Saved widget {idx} ({w}×{h}) from `{sel}`")
            except Exception as e:
                st.write(f"⚠️ Skipped a candidate from `{sel}`: {e}")

            step += 1
            prog.progress(min(step/total, 1.0))

        status.update(label="Extraction complete", state="complete")

    # Save an overlays.png for QA
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

    for i, row in selected.iterrows():
        with st.container(border=True):
            st.subheader(f"Extracting: {row['platform']} — {row['url']}")
            try:
                # Build session-specific prefix under the same day bucket hierarchy as page 21
                # Reuse the existing storage path to keep things grouped
                full_key = row["storage_path_full"]  # e.g., kpidrifthunter/2025/08/27/<session>/<slug>/full.png
                base_prefix = "/".join(full_key.split("/")[:-1])  # drop 'full.png'
                session_prefix = base_prefix

                res = extract_widgets(
                    url=row["url"],
                    screengrab_id=row["screengrab_id"],
                    full_key=full_key,
                    session_prefix=session_prefix
                )

                # Insert widget rows into DB using existing helper
                if res["crops"]:
                    insert_widgets(
                        sb,
                        screengrab_id=row["screengrab_id"],
                        storage_bucket=KDH_BUCKET,
                        crops=res["crops"],
                    )
                st.success(f"Extracted {res['widget_count']} widget(s).")
                st.link_button("Open overlays.png", storage_signed_url(KDH_BUCKET, res["overlays_key"]), use_container_width=False)

            except Exception as e:
                tb = traceback.format_exc()
                st.error(f"Failed to extract for {row['url']}\n\n{e}\n\n```traceback\n{tb}\n```")

    # Refresh the page data so you can run again
    st.toast("Done! Refreshing list…", icon="✅")
    time.sleep(1.2)
    st.cache_data.clear()
    st.rerun()
