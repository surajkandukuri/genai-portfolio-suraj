# -*- coding: utf-8 -*-
from __future__ import annotations

import os, io, base64, re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from PIL import Image
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from provisioning.a2_kpidrift_capture.a2_kpidrift_quality import (
    score_widget, append_quality_suffix
)
from provisioning.bootstrap import ensure_playwright_installed
ensure_playwright_installed()

# ───────── helpers ─────────
def _nowstamp_z() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def _sanitize(s: str, max_len: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-. ]+", "_", s)
    s = re.sub(r"\s+", "_", s)
    return (s[:max_len] or "untitled").rstrip("._-")

def _best_report_name_from_url(url: str) -> str:
    try:
        from urllib.parse import urlparse, unquote
        p = urlparse(url or "")
        segs = [s for s in (p.path or "").split("/") if s]
        if segs:
            return _sanitize(unquote(segs[-1]))
    except Exception:
        pass
    return "tableau_report"

# Simple wrapper to load Tableau JS API and call ws.getImageAsync()
_WRAPPER_HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Tableau Wrapper</title>
<style>html,body{margin:0;height:100%;width:100%;background:#fff;}#viz{position:absolute;inset:0;}</style>
<script src="https://public.tableau.com/javascripts/api/tableau-2.min.js"></script>
</head>
<body>
<div id="viz"></div>
<script>
window.__initViz = function(vizUrl, opts){
  return new Promise((resolve,reject)=>{
    try{
      const el=document.getElementById('viz');
      const viz=new tableau.Viz(el, vizUrl, Object.assign({hideTabs:false, hideToolbar:true, width:'100%', height:'100%'}, opts||{}));
      viz.addEventListener(tableau.TableauEventName.FIRST_INTERACTIVE, ()=>resolve(viz));
    }catch(e){reject(e);}
  });
};
</script>
</body>
</html>
"""

# ───────── public extractor (Power BI–style paths) ─────────
def extract_tableau_public(
    url: str,
    session_folder: Optional[str] = None,
    viewport: Tuple[int,int] = (1920,1080),
    scale: float = 2.0,
    max_widgets: int = 80,
) -> Dict:
    """
    Writes to:
      screenshots/tableau_<YYYYMMDDTHHMMSSZ>/widgets/
        tableau_<workbook>_<view>_<NN>[_good|_junk].png

    Returns:
      {
        "workbook": "<wb title>",
        "exported": [
          {"view_name","path","local_path","w","h"}, ...
        ],
        "session_prefix": "tableau_<YYYYMMDDTHHMMSSZ>"
      }
    """
    ts = _nowstamp_z()
    session_prefix = session_folder or f"tableau_{ts}"  # mirror powerbi_<ts>

    base_dir    = _ensure_dir(Path("screenshots") / session_prefix)
    widgets_dir = _ensure_dir(base_dir / "widgets")
    exported: List[Dict] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": viewport[0], "height": viewport[1]},
            device_scale_factor=scale
        )
        page = ctx.new_page()

        # Try JS API path first (per-worksheet images)
        workbook_name = None
        js_panels: List[Dict] = []
        try:
            page.set_content(_WRAPPER_HTML, wait_until="domcontentloaded")
            page.evaluate("""async (vizUrl) => { window.__viz = await window.__initViz(vizUrl, {}); }""", url)

            result = page.evaluate("""async () => {
                try{
                    const viz = window.__viz;
                    const wb  = viz.getWorkbook();
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
                }catch(e){ return {error:String(e)}; }
            }""")

            if result and not result.get("error"):
                workbook_name = (result.get("reportName") or "").strip() or None
                for p in (result.get("panels") or [])[:max_widgets]:
                    dn = p.get("dataUrl") or ""
                    nm = (p.get("name") or "Worksheet").strip() or "Worksheet"
                    if dn.startswith("data:image/png;base64,"):
                        b64 = dn.split(",", 1)[1]
                        png_bytes = base64.b64decode(b64)
                        js_panels.append({"name": nm, "bytes": png_bytes})
        except Exception:
            pass

        # If JS API succeeded, export all worksheet images
        if js_panels:
            wb = workbook_name or _best_report_name_from_url(url)
            wb_safe = _sanitize(wb)
            for idx, panel in enumerate(js_panels, start=1):
                title = panel["name"] or f"Worksheet_{idx}"
                fname_base = f"tableau_{wb_safe}_{_sanitize(title)}_{idx:02d}.png"

                png_bytes = panel["bytes"]
                try:
                    with Image.open(io.BytesIO(png_bytes)) as im:
                        w, h = im.size
                except Exception:
                    w, h = (1200, 800)

                q = score_widget("tableau_jsapi", (0,0,w,h), title_present=True)
                fname = append_quality_suffix(fname_base, q["quality"])

                out_path = widgets_dir / fname
                out_path.write_bytes(png_bytes)

                exported.append({
                    "view_name": title,
                    "path": f"{session_prefix}/widgets/{fname}",
                    "local_path": str(out_path),
                    "w": w, "h": h
                })

            ctx.close(); browser.close()
            return {
                "workbook": wb,
                "exported": exported,
                "session_prefix": session_prefix,
            }

        # ── Fallback: screenshot the embedded iframe (single widget) ──
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
            el = None
            for sel in OUTER_SELECTORS:
                try:
                    page.locator(sel).first.wait_for(state="visible", timeout=4000)
                    el = page.locator(sel).first
                    break
                except Exception:
                    continue

            wb = workbook_name or _best_report_name_from_url(url)
            wb_safe = _sanitize(wb)

            if el is not None:
                png_bytes = el.screenshot(type="png")
                try:
                    with Image.open(io.BytesIO(png_bytes)) as im:
                        w, h = im.size
                except Exception:
                    w, h = (1600, 900)

                fname_base = f"tableau_{wb_safe}_Widget_01.png"
                q = score_widget("tableau", (0,0,w,h), title_present=False)
                fname = append_quality_suffix(fname_base, q["quality"])

                out_path = widgets_dir / fname
                out_path.write_bytes(png_bytes)

                exported.append({
                    "view_name": "Widget",
                    "path": f"{session_prefix}/widgets/{fname}",
                    "local_path": str(out_path),
                    "w": w, "h": h
                })
        except Exception:
            pass
        finally:
            ctx.close(); browser.close()

    return {
        "workbook": workbook_name or _best_report_name_from_url(url),
        "exported": exported,
        "session_prefix": session_prefix,
    }
