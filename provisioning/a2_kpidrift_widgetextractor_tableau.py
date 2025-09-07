# provisioning/a2_kpidrift_widgetextractor_tableau.py
from __future__ import annotations

import os, re, json, uuid, time, io
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
from PIL import Image

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Error as PWError
from supabase import create_client, Client

# Quality helpers (shared)
from provisioning.a2_kpidrift_capture.a2_kpidrift_quality import (
    MIN_W, MIN_H, QUALITY_THRESHOLD,
    score_widget, append_quality_suffix,
)

# ── Config & Secrets ─────────────────────────────────────────────────────────
def _sget(*keys, default=None):
    # Env first (works both inside/outside Streamlit)
    for k in keys:
        v = os.getenv(k)
        if v:
            return v
    # Try Streamlit secrets if available
    try:
        import streamlit as st  # optional
        for k in keys:
            try:
                if k in st.secrets:
                    return st.secrets[k]
            except Exception:
                pass
    except Exception:
        pass
    return default

SUPABASE_URL  = _sget("SUPABASE_URL", "SUPABASE__URL")
SUPABASE_KEY  = _sget("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SERVICE_KEY", "SUPABASE_ANON_KEY", "SUPABASE__SUPABASE_SERVICE_KEY")

KDH_BUCKET             = _sget("KDH_BUCKET", default="kpidrifthunter")
KDH_FOLDER_ROOT        = _sget("KDH_FOLDER_ROOT", default="widgetextractor")
KDH_TABLE_SCREENGRABS  = _sget("KDH_TABLE_SCREENGRABS", default="kdh_screengrab_dim")
KDH_TABLE_WIDGETS      = _sget("KDH_TABLE_WIDGETS", default="kdh_widget_dim")

sb: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        sb = None

# ── Small helpers ─────────────────────────────────────────────────────────────
def _nowstamp() -> str:
    from datetime import datetime
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _ensure_outdir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def _sanitize_filename(s: str, max_len: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-. ]+", "_", s)
    s = re.sub(r"\s+", "_", s)
    return (s[:max_len] or "untitled").rstrip("._-")

def _slugify(s: str) -> str:
    return _sanitize_filename((s or "").lower()).strip("_")

def _storage_upload_bytes(bucket: str, key: str, data: bytes, content_type="image/png") -> Dict[str, str]:
    """Upload bytes to Supabase Storage; return {'key', 'public_url'}."""
    if not sb:
        return {"key": "", "public_url": ""}
    key = key.lstrip("/")
    try:
        sb.storage.from_(bucket).upload(path=key, file=data, file_options={"content-type": content_type, "upsert": True})
    except Exception:
        try:
            sb.storage.from_(bucket).remove([key])
        except Exception:
            pass
        sb.storage.from_(bucket).upload(path=key, file=data, file_options={"content-type": content_type})
    try:
        url = sb.storage.from_(bucket).get_public_url(key)
    except Exception:
        url = ""
    return {"key": key, "public_url": url}

def _db_insert(table: str, row: Dict) -> Dict:
    if not sb:
        return {"ok": False, "error": "no supabase client"}
    try:
        res = sb.table(table).insert(row).execute()
        return {"ok": True, "data": getattr(res, "data", None)}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _detect_report_name_host(page) -> str:
    """Try to get a decent report name from host page (outside the iframe)."""
    try:
        og = page.locator("meta[property='og:title']").first
        tw = page.locator("meta[name='twitter:title']").first
        title = page.title() or ""
        vals = []
        if og and og.count(): vals.append(og.get_attribute("content") or "")
        if tw and tw.count(): vals.append(tw.get_attribute("content") or "")
        if title: vals.append(title)
        vals = [v.strip() for v in vals if v and v.strip()]
        if vals:
            # pick the longest non-generic
            vals.sort(key=len, reverse=True)
            return _sanitize_filename(vals[0])
    except Exception:
        pass
    # Fallback: URL segment
    try:
        from urllib.parse import urlparse
        p = urlparse(page.url or "")
        seg = (p.path or "").rstrip("/").split("/")[-1]
        if seg.lower() in ("view","vizhome"): seg = ""
        return _sanitize_filename(seg or (p.netloc or "tableau_report"))
    except Exception:
        return "tableau_report"

# ── Image segmentation (pure PIL+NumPy; no OpenCV dependency) ────────────────
def _find_gutter_runs(mask: np.ndarray, min_run: int) -> List[Tuple[int, int]]:
    """
    Given a 1D boolean mask, return [(start, end_exclusive), ...] for contiguous True runs
    whose length >= min_run.
    """
    runs: List[Tuple[int, int]] = []
    if mask.size == 0:
        return runs
    in_run = False
    start = 0
    for i, v in enumerate(mask):
        if v and not in_run:
            in_run = True
            start = i
        elif not v and in_run:
            if i - start >= min_run:
                runs.append((start, i))
            in_run = False
    if in_run:
        if mask.size - start >= min_run:
            runs.append((start, mask.size))
    return runs

def _grid_boundaries_from_gutters(W: int, H: int,
                                  v_gutters: List[Tuple[int, int]],
                                  h_gutters: List[Tuple[int, int]]) -> Tuple[List[int], List[int]]:
    """
    Convert gutter runs into column and row boundaries.
    We place boundaries at 0, midpoints of gutters, and W/H respectively.
    """
    cols = [0]
    rows = [0]
    for s, e in v_gutters:
        m = (s + e) // 2
        if 6 < m < W - 6 and (len(cols) == 0 or abs(cols[-1] - m) > 12):
            cols.append(m)
    for s, e in h_gutters:
        m = (s + e) // 2
        if 6 < m < H - 6 and (len(rows) == 0 or abs(rows[-1] - m) > 12):
            rows.append(m)
    cols.append(W)
    rows.append(H)
    cols = sorted(set(cols))
    rows = sorted(set(rows))
    return cols, rows

def _segment_panels(img: Image.Image,
                    min_panel_w: int = 320,
                    min_panel_h: int = 180,
                    min_panel_area: int = 120_000,
                    light_thresh: int = 240,
                    v_gutter_ratio: float = 0.92,
                    h_gutter_ratio: float = 0.92,
                    min_gutter_px: int = 18) -> List[Tuple[int,int,int,int]]:
    """
    Returns a list of rectangles (x,y,w,h) in image coordinates representing panel candidates.
    Strategy:
    - Threshold near-white (gutters).
    - Use vertical/horizontal projection to find gutter runs.
    - Build a grid, emit rectangles between gutters.
    - Filter by size and non-white content.
    """
    gray = img.convert("L")
    A = np.asarray(gray)  # H x W
    H, W = A.shape

    # near-white mask
    white = (A >= light_thresh)

    # proportion of white by column/row
    v_prop = white.sum(axis=0) / H  # length W
    h_prop = white.sum(axis=1) / W  # length H

    v_mask = v_prop >= v_gutter_ratio
    h_mask = h_prop >= h_gutter_ratio

    v_runs = _find_gutter_runs(v_mask, min_run=min_gutter_px)
    h_runs = _find_gutter_runs(h_mask, min_run=min_gutter_px)

    cols, rows = _grid_boundaries_from_gutters(W, H, v_runs, h_runs)

    rects: List[Tuple[int,int,int,int]] = []
    for ci in range(len(cols)-1):
        x1, x2 = cols[ci], cols[ci+1]
        for ri in range(len(rows)-1):
            y1, y2 = rows[ri], rows[ri+1]
            w = x2 - x1
            h = y2 - y1
            if w < min_panel_w or h < min_panel_h or (w*h) < min_panel_area:
                continue
            # Non-white content ratio to avoid empty gutters being counted
            sub = A[y1:y2, x1:x2]
            nonwhite_ratio = (sub < 250).mean()
            if nonwhite_ratio < 0.20:
                continue
            rects.append((x1, y1, w, h))

    # De-duplicate overlapping rectangles, prefer larger areas
    def iou(a, b):
        ax, ay, aw, ah = a; bx, by, bw, bh = b
        x1, y1 = max(ax, bx), max(ay, by)
        x2, y2 = min(ax+aw, bx+bw), min(ay+ah, by+bh)
        inter = max(0, x2-x1) * max(0, y2-y1)
        if inter <= 0: return 0.0
        ua = aw*ah + bw*bh - inter
        return inter / max(1, ua)

    kept: List[Tuple[int,int,int,int]] = []
    for r in sorted(rects, key=lambda rr: rr[2]*rr[3], reverse=True):
        drop = False
        for k in kept:
            if iou(r, k) > 0.2:
                drop = True; break
        if not drop:
            kept.append(r)

    return kept

# ── Public API (Tableau) ─────────────────────────────────────────────────────
def extract(url: str, session_folder: str, viewport=(1920,1080), scale=2.0, max_widgets=80,
            segmentation: str = "auto") -> Dict:
    """
    Tableau extractor (cross-origin safe).
    - Discovers the OUTER Tableau container(s) on the host page
    - Crops using element_handle.screenshot() to avoid frame offset issues
    - If segmentation='auto', splits big canvas into multiple panels using image heuristics
    - Saves local copy, uploads to Supabase, and inserts DB rows
    - Returns manifest compatible with the Power BI extractor
    """
    base_local = _ensure_outdir(Path("./screenshots") / session_folder)
    outdir_widgets = _ensure_outdir(base_local / "widgets")

    platform = "tableau"
    ts = _nowstamp()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": viewport[0], "height": viewport[1]},
                                  device_scale_factor=scale)
        page = ctx.new_page()

        # Navigate
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        # Let Tableau's lazy loader settle
        try:
            page.wait_for_load_state("networkidle", timeout=6_000)
        except PWTimeout:
            pass

        # Scroll a bit to trigger rendering if necessary
        try:
            page.mouse.wheel(0, 600)
            page.wait_for_timeout(800)
        except Exception:
            pass

        # Wait for outer containers to appear
        OUTER_SELECTORS = [
            "iframe[src*='public.tableau.com']",
            "object[type='text/html'][data*='tableau']",
            ".tableauPlaceholder",
            "[aria-label*='Tableau']",
        ]
        found_any = False
        chosen_sel = None
        for sel in OUTER_SELECTORS:
            try:
                page.locator(sel).first.wait_for(state="visible", timeout=4000)
                found_any = True
                chosen_sel = sel
                break
            except Exception:
                continue
        # extra settle
        page.wait_for_timeout(800)

        # Compute report name
        report_name = _detect_report_name_host(page)
        report_slug = _slugify(report_name) or "tableau_report"

        # Full page screenshot (host)
        full_filename = f"{platform}_full_{ts}.png"
        full_local_path = base_local / full_filename
        page.screenshot(path=str(full_local_path), full_page=True)

        # Upload full page
        full_key = f"{KDH_FOLDER_ROOT}/{session_folder}/{full_filename}"
        uploaded_full = _storage_upload_bytes(KDH_BUCKET, full_key, full_local_path.read_bytes())

        # Insert screengrab row
        screengrab_row = {
            "screengrab_id": str(uuid.uuid4()),
            "url": url,
            "platform": platform,
            "report_name": report_name,
            "report_slug": report_slug,
            "storage_path_full": uploaded_full.get("key",""),
            "public_url_full": uploaded_full.get("public_url",""),
            "captured_at": ts,
        }
        _db_insert(KDH_TABLE_SCREENGRABS, screengrab_row)

        if not found_any or chosen_sel is None:
            # Nothing visible; return empty manifest with just screengrab row info
            return {
                "url": url,
                "platform": platform,
                "report_name": report_name,
                "report_slug": report_slug,
                "captured_at": ts,
                "session_folder": session_folder,
                "widgets_count": 0,
                "widgets": [],
                "full_path_local": str(full_local_path.resolve()),
                "storage_prefix": f"{KDH_FOLDER_ROOT}/{session_folder}/",
            }

        # Discover OUTER candidates on the HOST page (safe for cross-origin)
        PAD = 12
        candidates: List[Tuple[str, Tuple[int,int,int,int]]] = []
        try:
            loc = page.locator(chosen_sel)
            n = min(6, loc.count())
            for i in range(n):
                el = loc.nth(i)
                try:
                    bb = el.bounding_box()
                    if not bb: continue
                    x, y = int(bb["x"]), int(bb["y"])
                    w, h = int(bb["width"]), int(bb["height"])
                    if w < MIN_W or h < MIN_H: continue
                    candidates.append((chosen_sel, (x, y, w, h)))
                except Exception:
                    pass
        except PWError:
            pass

        # Prefer the largest candidate (public embeds are usually one big viz)
        if not candidates:
            candidates = []
        candidates = sorted(candidates, key=lambda c: c[1][2]*c[1][3], reverse=True)

        # Save & upload widget crops
        widgets_saved = []
        idx_counter = 1

        for sel, (x, y, w, h) in candidates[:1]:  # usually 1 big container
            # Take element screenshot as bytes
            chosen = page.locator(sel).first
            try:
                png_bytes = chosen.screenshot(type="png")
            except Exception:
                # Fallback to page clip using outer bbox
                tmp_path = base_local / f"__tbl_{_nowstamp()}_tmp.png"
                page.screenshot(path=str(tmp_path), clip={"x": x, "y": y, "width": w, "height": h})
                png_bytes = tmp_path.read_bytes()
                try: tmp_path.unlink()
                except: pass

            # If segmentation enabled, split into panels
            panel_rects: List[Tuple[int,int,int,int]] = []
            if segmentation == "auto":
                try:
                    img = Image.open(io.BytesIO(png_bytes))
                    panel_rects = _segment_panels(img)
                except Exception:
                    panel_rects = []

            # If we found fewer than 2 panels, just use the whole container
            if len(panel_rects) < 2:
                panel_rects = [(0, 0, w, h)]
                # adjust to image dimensions if fallback shot size != bbox size
                try:
                    img = Image.open(io.BytesIO(png_bytes))
                    iw, ih = img.size
                    panel_rects = [(0, 0, iw, ih)]
                except Exception:
                    pass

            # Emit each panel as a widget
            try:
                img = Image.open(io.BytesIO(png_bytes))
                iw, ih = img.size
            except Exception:
                img = None
                iw, ih = (w, h)

            for (px, py, pw, ph) in panel_rects[:max_widgets]:
                # Defensive clamp within image
                px = max(0, min(px, iw-1))
                py = max(0, min(py, ih-1))
                pw = max(1, min(pw, iw - px))
                ph = max(1, min(ph, ih - py))

                # Map image coords back to page coords
                gx, gy, gw, gh = x + px, y + py, pw, ph

                # Title discovery: cross-origin → skip (use generic)
                title = "Widget"
                title_stub = _sanitize_filename(title)
                base_filename = f"{platform}_{report_name}_{title_stub}_{idx_counter:02d}.png"

                # Quality score (selector_kind='tableau', no title)
                qinfo = score_widget(
                    selector_kind="tableau",
                    bbox_xywh=(gx, gy, gw, gh),
                    title_present=False,
                )
                widget_filename = append_quality_suffix(base_filename, qinfo["quality"])
                local_path = outdir_widgets / widget_filename

                # Crop from the panel rectangle
                if img is not None:
                    crop = img.crop((px, py, px+pw, py+ph))
                    buf = io.BytesIO()
                    crop.save(buf, format="PNG")
                    local_path.write_bytes(buf.getvalue())
                else:
                    # Fallback to page clip
                    page.screenshot(path=str(local_path), clip={"x": gx, "y": gy, "width": gw, "height": gh})

                # Upload
                widget_key = f"{KDH_FOLDER_ROOT}/{session_folder}/widgets/{widget_filename}"
                uploaded_w = _storage_upload_bytes(KDH_BUCKET, widget_key, local_path.read_bytes())

                extraction_notes = {
                    "quality": qinfo["quality"],
                    "quality_score": qinfo["quality_score"],
                    "quality_reason": qinfo["quality_reason"],
                    "selector_kind": "tableau",
                    "title_present": False,
                    "area_px": qinfo["area_px"],
                    "threshold": QUALITY_THRESHOLD,
                    "segmentation": segmentation,
                    "panel_box_image_xywh": [px, py, pw, ph],
                }

                widget_row = {
                    "widget_id": str(uuid.uuid4()),
                    "url": url,
                    "platform": platform,
                    "report_name": report_name,
                    "report_slug": report_slug,
                    "widget_title": title,
                    "widget_index": idx_counter,
                    "bbox": [gx, gy, gw, gh],
                    "storage_path_widget": uploaded_w.get("key",""),
                    "public_url_widget": uploaded_w.get("public_url",""),
                    "captured_at": ts,
                    "session_folder": session_folder,
                    "extraction_notes": json.dumps(extraction_notes),
                }
                _db_insert(KDH_TABLE_WIDGETS, widget_row)

                widgets_saved.append({
                    "idx": idx_counter,
                    "title": title,
                    "bbox": [gx, gy, gw, gh],
                    "local_path": str(local_path),
                    "quality": qinfo["quality"],
                    "quality_score": qinfo["quality_score"],
                })

                idx_counter += 1

        ctx.close(); browser.close()

    return {
        "url": url,
        "platform": platform,
        "report_name": report_name,
        "report_slug": report_slug,
        "captured_at": ts,
        "session_folder": session_folder,
        "widgets_count": len(widgets_saved),
        "widgets": widgets_saved,
        "full_path_local": str((Path("./screenshots")/session_folder/(f'{platform}_full_{ts}.png')).resolve()),
        "storage_prefix": f"{KDH_FOLDER_ROOT}/{session_folder}/",
    }
