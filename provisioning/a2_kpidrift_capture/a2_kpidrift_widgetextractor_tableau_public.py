from __future__ import annotations

import os, re, io, json, uuid, base64
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from PIL import Image
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from supabase import create_client, Client

# Quality helpers (shared)
from provisioning.a2_kpidrift_capture.a2_kpidrift_quality import (
    QUALITY_THRESHOLD,
    score_widget,
    append_quality_suffix,
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

# Use same variable names/structure as the rest of the app
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

def _best_report_name_from_url(url: str) -> str:
    try:
        from urllib.parse import urlparse, unquote
        p = urlparse(url or "")
        segs = [s for s in (p.path or "").split("/") if s]
        if segs:
            return _sanitize_filename(unquote(segs[-1]))
    except Exception:
        pass
    return "tableau_report"

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

# ── JS API wrapper HTML (host page) ──────────────────────────────────────────
_WRAPPER_HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Tableau Wrapper</title>
<style>
html,body{margin:0;padding:0;height:100%;width:100%;background:#fff;}
#viz{position:absolute;inset:0;}
</style>
<script src="https://public.tableau.com/javascripts/api/tableau-2.min.js"></script>
</head>
<body>
<div id="viz"></div>
<script>
window.__initViz = function(vizUrl, opts){
    return new Promise((resolve, reject) => {
        try{
            const el = document.getElementById('viz');
            const viz = new tableau.Viz(el, vizUrl, Object.assign({hideTabs:false, hideToolbar:true, width:'100%', height:'100%'}, opts||{}));
            viz.addEventListener(tableau.TableauEventName.FIRST_INTERACTIVE, () => resolve(viz));
        }catch(e){ reject(e); }
    });
}
</script>
</body>
</html>
"""

# ──────────────────────────────────────────────────────────────────────────────
# Public extractor (JS API first; fallback to container screenshot)
# ──────────────────────────────────────────────────────────────────────────────
def extract_tableau_public(
    *,
    url: str,
    session_folder: str,
    viewport: Tuple[int, int] = (1920, 1080),
    scale: float = 2.0,
    max_widgets: int = 80,
) -> Dict:
    """
    Extract widgets from a Tableau **Public** dashboard.
    - Preferred path: embed JS API → enumerate worksheets → getImageAsync()
    - Fallback: screenshot the outer public iframe (single widget)
    Everything saves under:
        ./screenshots/<session_folder>/widgets/
    and uploads to:
        KDH_FOLDER_ROOT/<session_folder>/widgets/<filename>.png
    """
    base_local = _ensure_outdir(Path("./screenshots") / session_folder)
    outdir_widgets = _ensure_outdir(base_local / "widgets")

    ts = _nowstamp()
    platform = "tableau_public"

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": viewport[0], "height": viewport[1]},
            device_scale_factor=scale,
        )
        page = ctx.new_page()

        # Render our wrapper then init the viz via JS API
        page.set_content(_WRAPPER_HTML, wait_until="domcontentloaded")

        report_name_js = None
        extracted_panels: List[Dict] = []
        jsapi_ok = False

        try:
            # Initialize viz and wait for FIRST_INTERACTIVE
            page.evaluate("""async (vizUrl) => {
                window.__viz = await window.__initViz(vizUrl, {});
            }""", url)

            result = page.evaluate("""async () => {
                try{
                    const viz = window.__viz;
                    const wb = viz.getWorkbook();
                    let reportName = '';
                    try { reportName = wb.getName && wb.getName() || ''; } catch(e){}
                    const active = wb.getActiveSheet();
                    const sheets = (active.getSheetType && active.getSheetType() === 'dashboard')
                        ? active.getWorksheets()
                        : [active];

                    const out = [];
                    for (const ws of sheets){
                        const name = (ws.getName && ws.getName()) || 'Worksheet';
                        const dataUrl = await ws.getImageAsync(); // data:image/png;base64,...
                        out.push({name, dataUrl});
                    }
                    return {reportName, panels: out};
                }catch(e){
                    return {error: String(e)};
                }
            }""")

            if result and not result.get("error"):
                report_name_js = (result.get("reportName") or "").strip() or None
                panels = result.get("panels") or []
                for p in panels[:max_widgets]:
                    data_url = p.get("dataUrl") or ""
                    name = (p.get("name") or "Worksheet").strip() or "Worksheet"
                    if data_url.startswith("data:image/png;base64,"):
                        b64 = data_url.split(",", 1)[1]
                        png_bytes = base64.b64decode(b64)
                        extracted_panels.append({"name": name, "bytes": png_bytes})
                jsapi_ok = len(extracted_panels) > 0
        except Exception:
            jsapi_ok = False

        # Compute report name/slug; filenames use neutral 'tableau_' prefix
        report_name = _best_report_name_from_url(url) if not report_name_js else _sanitize_filename(report_name_js)
        report_slug = _slugify(report_name) or "tableau_report"

        # Full-page screenshot (wrapper) for screengrab table
        full_filename = f"tableau_full_{ts}.png"
        full_local_path = base_local / full_filename
        try:
            page.screenshot(path=str(full_local_path), full_page=True)
        except Exception:
            pass

        # Upload full page
        full_key = f"{KDH_FOLDER_ROOT}/{session_folder}/{full_filename}"
        uploaded_full = _storage_upload_bytes(KDH_BUCKET, full_key, full_local_path.read_bytes() if full_local_path.exists() else b"")

        # DB row: screengrab
        _db_insert(KDH_TABLE_SCREENGRABS, {
            "screengrab_id": str(uuid.uuid4()),
            "url": url,
            "platform": platform,
            "report_name": report_name,
            "report_slug": report_slug,
            "storage_path_full": uploaded_full.get("key",""),
            "public_url_full": uploaded_full.get("public_url",""),
            "captured_at": ts,
        })

        widgets_saved: List[Dict] = []

        if jsapi_ok:
            # One widget per worksheet image
            idx = 1
            for panel in extracted_panels:
                title = panel.get("name") or "Worksheet"
                title_stub = _sanitize_filename(title)

                base_filename = f"tableau_{report_slug}_{title_stub}_{idx:02d}.png"
                png_bytes = panel["bytes"]

                # Read dimensions from bytes for bbox
                try:
                    with Image.open(io.BytesIO(png_bytes)) as im:
                        w, h = im.size
                except Exception:
                    w, h = (800, 600)

                # Score and name with suffix
                qinfo = score_widget("tableau_jsapi", (0, 0, w, h), title_present=True)
                widget_filename = append_quality_suffix(base_filename, qinfo["quality"])

                # Save local
                local_path = outdir_widgets / widget_filename
                local_path.write_bytes(png_bytes)

                # Upload
                widget_key = f"{KDH_FOLDER_ROOT}/{session_folder}/widgets/{widget_filename}"
                uploaded_w = _storage_upload_bytes(KDH_BUCKET, widget_key, local_path.read_bytes())

                # DB row
                _db_insert(KDH_TABLE_WIDGETS, {
                    "widget_id": str(uuid.uuid4()),
                    "url": url,
                    "platform": platform,
                    "report_name": report_name,
                    "report_slug": report_slug,
                    "widget_title": title,
                    "widget_index": idx,
                    "bbox": [0, 0, w, h],
                    "storage_path_widget": uploaded_w.get("key",""),
                    "public_url_widget": uploaded_w.get("public_url",""),
                    "captured_at": ts,
                    "session_folder": session_folder,
                    "extraction_notes": json.dumps({
                        "quality": qinfo["quality"],
                        "quality_score": qinfo["quality_score"],
                        "quality_reason": qinfo.get("quality_reason",""),
                        "selector_kind": "tableau_jsapi",
                        "title_present": True,
                        "area_px": w*h,
                        "threshold": QUALITY_THRESHOLD,
                        "source": "tableau_jsapi",
                        "worksheet_title": title,
                    }),
                })

                widgets_saved.append({
                    "idx": idx,
                    "title": title,
                    "bbox": [0, 0, w, h],
                    "local_path": str(local_path),
                    "quality": qinfo["quality"],
                    "quality_score": qinfo["quality_score"],
                })
                idx += 1

        else:
            # Fallback: screenshot the outer public iframe (single widget)
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                try:
                    page.wait_for_load_state("networkidle", timeout=6_000)
                except PWTimeout:
                    pass

                OUTER_SELECTORS = [
                    "iframe[src*='public.tableau.com']",
                    "object[type='text/html'][data*='tableau']",
                    ".tableauPlaceholder",
                    "[aria-label*='Tableau']",
                ]
                chosen_sel = None
                for sel in OUTER_SELECTORS:
                    try:
                        page.locator(sel).first.wait_for(state="visible", timeout=4000)
                        chosen_sel = sel
                        break
                    except Exception:
                        continue

                if chosen_sel:
                    el = page.locator(chosen_sel).first
                    png_bytes = el.screenshot(type="png")
                    with Image.open(io.BytesIO(png_bytes)) as im:
                        w, h = im.size

                    base_filename = f"tableau_{report_slug}_widget_01.png"
                    qinfo = score_widget("tableau", (0, 0, w, h), title_present=False)
                    widget_filename = append_quality_suffix(base_filename, qinfo["quality"])

                    local_path = outdir_widgets / widget_filename
                    local_path.write_bytes(png_bytes)

                    widget_key = f"{KDH_FOLDER_ROOT}/{session_folder}/widgets/{widget_filename}"
                    uploaded_w = _storage_upload_bytes(KDH_BUCKET, widget_key, local_path.read_bytes())

                    _db_insert(KDH_TABLE_WIDGETS, {
                        "widget_id": str(uuid.uuid4()),
                        "url": url,
                        "platform": platform,
                        "report_name": report_name,
                        "report_slug": report_slug,
                        "widget_title": "Widget",
                        "widget_index": 1,
                        "bbox": [0, 0, w, h],
                        "storage_path_widget": uploaded_w.get("key",""),
                        "public_url_widget": uploaded_w.get("public_url",""),
                        "captured_at": ts,
                        "session_folder": session_folder,
                        "extraction_notes": json.dumps({
                            "quality": qinfo["quality"],
                            "quality_score": qinfo["quality_score"],
                            "quality_reason": qinfo.get("quality_reason",""),
                            "selector_kind": "tableau",
                            "title_present": False,
                            "area_px": w*h,
                            "threshold": QUALITY_THRESHOLD,
                            "source": "fallback_outer_container",
                        }),
                    })

                    widgets_saved.append({
                        "idx": 1,
                        "title": "Widget",
                        "bbox": [0, 0, w, h],
                        "local_path": str(local_path),
                        "quality": qinfo["quality"],
                        "quality_score": qinfo["quality_score"],
                    })
            except Exception:
                pass

        ctx.close()
        browser.close()

    return {
        "mode": "public",
        "url": url,
        "platform": platform,
        "report_name": report_name,
        "report_slug": report_slug,
        "captured_at": ts,
        "session_folder": session_folder,
        "widgets_count": len(widgets_saved),
        "widgets": widgets_saved,
        "full_path_local": str((Path("./screenshots")/session_folder/(f'tableau_full_{ts}.png')).resolve()),
        "storage_prefix": f"{KDH_FOLDER_ROOT}/{session_folder}/",
    }
