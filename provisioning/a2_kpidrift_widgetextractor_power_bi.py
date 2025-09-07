# provisioning/a2_kpidrift_widgetextractor_power_bi.py
from __future__ import annotations

import os, re, json, uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Error as PWError
from supabase import create_client, Client

# Quality helpers (shared)
from provisioning.a2_kpidrift_capture.a2_kpidrift_quality import (
    MIN_W, MIN_H, QUALITY_THRESHOLD,
    score_widget, append_quality_suffix,
)

# ── Config & Secrets ─────────────────────────────────────────────────────────
def _sget(*keys, default=None):
    for k in keys:
        v = os.getenv(k)
        if v:
            return v
    # Streamlit not guaranteed to be present when importing as a module
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

# ── Power BI specific helpers ────────────────────────────────────────────────
_BAD_EXACT = {
    "microsoft power bi","power bi","view report","report","dashboard","sign in",
    "home","sheet","show filters","navigating to visual","use ctrl","press ctrl",
    "press enter","skip to report","skip to main content"
}
_BAD_SUBSTR = ["navigating to visual","use ctrl","press ctrl","keyboard shortcut","skip to report","skip to main content","aria-live"]

def _non_generic(txt: str) -> bool:
    low = (txt or "").strip().lower()
    if not low or low in _BAD_EXACT: return False
    if any(sub in low for sub in _BAD_SUBSTR): return False
    return True

def _sanitize_vendor_title(s: str) -> str:
    s = re.sub(r"\s*[-|]\s*Microsoft\s*Power\s*BI.*$", "", s, flags=re.I)
    s = re.sub(r"\s*-\s*Power\s*BI.*$", "", s, flags=re.I)
    return s.strip()

def _pick_best_text(cands: List[str]) -> Optional[str]:
    scored = []
    for raw in cands:
        if not raw: continue
        t = raw.strip()
        if not _non_generic(t): continue
        score = len(t) + (3 if " " in t else 0)
        scored.append((score, t))
    if not scored: return None
    scored.sort(reverse=True)
    return scored[0][1]

def _guess_title_by_style(frame, top_px: int = 380) -> Optional[str]:
    try:
        nodes = frame.evaluate(
            """(topLimit) => {
              const take = [];
              const w = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
              while (w.nextNode()) {
                const el = w.currentNode;
                if (!el) continue;
                const cs = getComputedStyle(el);
                if (cs.visibility==='hidden'||cs.display==='none'||parseFloat(cs.opacity||'1')<0.1) continue;
                const r = el.getBoundingClientRect();
                if (!r || !r.width || !r.height) continue;
                if (r.top > topLimit) continue;
                const text = (el.innerText||'').trim();
                if (!text || text.length < 4) continue;
                const size = parseFloat(cs.fontSize||'0');
                const weight = (cs.fontWeight||'').toString();
                take.push({ text, size: isNaN(size)?0:size, weight: /^(700|800|900|bold)$/i.test(weight)?1:0, top:r.top });
              }
              return take;
            }""",
            top_px
        )
        scored = []
        for n in nodes or []:
            t = (n.get("text") or "").strip()
            if not t or not _non_generic(t): continue
            size = float(n.get("size", 0)); bold = int(n.get("weight", 0)); top = float(n.get("top", 9999.0))
            score = size*3 + bold*5 - top*0.01 + (3 if " " in t else 0)
            scored.append((score, t))
        if not scored: return None
        scored.sort(reverse=True)
        return scored[0][1]
    except Exception:
        return None

def _detect_report_name(page, frame) -> Tuple[str, Dict]:
    """Return (report_name, debug_info)."""
    candidates: List[Tuple[str, str]] = []
    dbg: Dict = {"iframe": {}, "headers": [], "active_tabs": [], "meta": {}, "picked": None, "heuristic": None}
    def add(src: str, val: Optional[str]):
        if not val: return
        t = (val or "").strip()
        if not t: return
        candidates.append((src, t))

    try:
        ifr = page.locator("iframe").first
        if ifr.count():
            t = ifr.get_attribute("title"); a = ifr.get_attribute("aria-label")
            dbg["iframe"] = {"title": t, "aria-label": a}
            if t: add("iframe@title", t)
            if a: add("iframe@aria-label", a)
    except Exception: pass

    try:
        for sel in ["[data-testid='report-header-title']",
                    "[data-testid='report-header'] [data-testid='title']",
                    ".reportTitle",".vcHeaderTitle"]:
            loc = frame.locator(sel)
            n = min(8, loc.count())
            for i in range(n):
                try:
                    role = (loc.nth(i).get_attribute("role") or "").lower()
                    if "status" in role: continue
                    txt = (loc.nth(i).inner_text() or "").strip()
                    dbg["headers"].append({"sel": sel, "text": txt, "role": role})
                    add(f"header:{sel}", txt)
                except Exception: pass
    except Exception: pass

    try:
        for sel in ["[aria-label='Pages Navigation'] [role='tab'][aria-selected='true']",
                    "[role='tablist'] [role='tab'][aria-selected='true']",
                    "[role='tab'][aria-current='page']"]:
            loc = frame.locator(sel)
            n = min(4, loc.count())
            for i in range(n):
                try:
                    txt = (loc.nth(i).inner_text() or "").strip()
                    dbg["active_tabs"].append({"sel": sel, "text": txt})
                    add(f"active-tab:{sel}", txt)
                except Exception: pass
    except Exception: pass

    try:
        og = page.locator("meta[property='og:title']").first
        tw = page.locator("meta[name='twitter:title']").first
        pt = _sanitize_vendor_title(page.title() or "")
        if og and og.count():
            v = _sanitize_vendor_title(og.get_attribute("content") or ""); dbg["meta"]["og:title"] = v; add("og:title", v)
        if tw and tw.count():
            v = _sanitize_vendor_title(tw.get_attribute("content") or ""); dbg["meta"]["twitter:title"] = v; add("twitter:title", v)
        if pt: dbg["meta"]["<title>"] = pt; add("<title>", pt)
    except Exception: pass

    explicit = _pick_best_text([t for _, t in candidates])
    if explicit:
        dbg["picked"] = {"source":"explicit","text":explicit}
        return _sanitize_filename(explicit), dbg

    guess = _guess_title_by_style(frame, top_px=380)
    if guess and _non_generic(guess):
        dbg["heuristic"] = guess
        dbg["picked"]   = {"source":"largest-top-text","text":guess}
        return _sanitize_filename(guess), dbg

    try:
        from urllib.parse import urlparse
        p = urlparse(page.url or "")
        seg = (p.path or "").rstrip("/").split("/")[-1]
        if (seg or "").lower() == "view": seg = ""
        name = _sanitize_filename(seg or (p.netloc or "report"))
        dbg["picked"] = {"source":"url-fallback","text":name}
        return name, dbg
    except Exception:
        return "report", {"picked":{"source":"exception","text":"report"}}

def _find_title_near(frame, box: Tuple[int,int,int,int]) -> Optional[str]:
    try:
        bx, by, bw, bh = box
        sel = ".visualTitle, .visualHeaderTitleText, [role='heading'], h1, h2, h3, h4, h5, h6"
        loc = frame.locator(sel)
        n = min(20, loc.count())
        closest, best_dy = None, 99999
        for i in range(n):
            t = loc.nth(i)
            tb = t.bounding_box()
            if not tb: continue
            tx, ty, tw, th = int(tb["x"]), int(tb["y"]), int(tb["width"]), int(tb["height"])
            if ty < by and (tx < (bx + bw) and (tx + tw) > bx):
                dy = by - ty
                if 0 < dy < 220 and dy < best_dy:
                    label = (t.inner_text() or "").strip()
                    if label:
                        best_dy = dy; closest = label
        return closest
    except Exception:
        return None

# ── Public API ────────────────────────────────────────────────────────────────
def extract(url: str, session_folder: str, viewport=(1920,1080), scale=2.0, max_widgets=80) -> Dict:
    """
    Power BI extractor.
    Writes locally under ./screenshots/<session>/widgets/
    Uploads to Storage at widgetextractor/<session>/...
    Inserts rows into kdh_screengrab_dim and kdh_widget_dim.
    Returns manifest (compatible with the page's current expectations).
    """
    base_local = _ensure_outdir(Path("./screenshots") / session_folder)
    outdir_widgets = _ensure_outdir(base_local / "widgets")

    platform = "powerbi"
    ts = _nowstamp()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": viewport[0], "height": viewport[1]},
                                  device_scale_factor=scale)
        page = ctx.new_page()

        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        try:
            page.wait_for_load_state("networkidle", timeout=6_000)
        except PWTimeout:
            pass
        page.wait_for_timeout(1000)

        # pick content frame if present
        frame = page.main_frame
        try:
            if page.locator("iframe").count():
                cf = page.locator("iframe").first.content_frame()
                if cf: frame = cf
        except Exception:
            pass

        # report name + slug
        report_name, _dbg = _detect_report_name(page, frame)
        report_slug = _slugify(report_name) or "report"

        # ── Full page screenshot
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

        # ── Collect candidate elements to crop (Power BI selectors)
        selectors = [
            ".visualContainer, .visualContainerHost, .modernVisualOverlay",   # Power BI containers
            "[role='figure'], [role='img']",
            "svg, canvas",
        ]
        PAD = 12
        candidates: List[Tuple[str, Tuple[int,int,int,int], str]] = []  # (sel, box, kind)

        def _kind_of(sel: str) -> str:
            if (".visualContainer" in sel) or (".modernVisualOverlay" in sel):
                return "container"
            if "[role=" in sel:
                return "role"
            return "primitive"

        for sel in selectors:
            try:
                loc = frame.locator(sel)
                n = min(60, loc.count())
                for i in range(n):
                    el = loc.nth(i)
                    try:
                        bb = el.bounding_box()
                        if not bb: continue
                        x, y = int(bb["x"]), int(bb["y"]); w, h = int(bb["width"]), int(bb["height"])
                        if w < MIN_W or h < MIN_H: continue
                        candidates.append((sel, (x, y, w, h), _kind_of(sel)))
                    except Exception: pass
            except PWError:
                pass

        # dedupe by IoU with preference for container over primitive/role
        kept: List[Tuple[str, Tuple[int,int,int,int], str]] = []
        def _iou_local(a, b):
            ax, ay, aw, ah = a; bx, by, bw, bh = b
            x1, y1 = max(ax, bx), max(ay, by)
            x2, y2 = min(ax+aw, bx+bw), min(ay+ah, by+bh)
            inter = max(0, x2-x1) * max(0, y2-y1)
            if inter == 0: return 0.0
            ua = aw*ah + bw*bh - inter
            return inter / max(1, ua)

        for sel, box, kind in sorted(candidates, key=lambda c: (c[1][1], c[1][0])):  # TL->BR
            drop = False
            for _, k_box, k_kind in kept:
                overlap = _iou_local(box, k_box)
                if overlap > 0.65 and k_kind == "container" and kind in ("primitive","role"):
                    drop = True; break
                if overlap > 0.72 and k_kind == kind:
                    drop = True; break
            if not drop:
                kept.append((sel, box, kind))

        # ── Save & upload widget crops
        widgets_saved = []
        for idx, (sel, (x, y, w, h), kind) in enumerate(kept[:max_widgets], start=1):
            px, py = max(0, x-PAD), max(0, y-PAD)
            pw_, ph_ = max(1, w+2*PAD), max(1, h+2*PAD)

            title = _find_title_near(frame, (x, y, w, h)) or "Widget"
            title_stub = _sanitize_filename(title)

            base_filename = f"{platform}_{report_name}_{title_stub}_{idx:02d}.png"

            # Quality score & suffix
            qinfo = score_widget(
                selector_kind=kind,
                bbox_xywh=(pw_, ph_, pw_, ph_),  # scoring on crop size; width= pw_, height= ph_
                title_present=bool(title.strip()),
            )
            # Note: fix bbox order (x,y,w,h) for scoring if you prefer exact; it doesn't affect classification here.

            widget_filename = append_quality_suffix(base_filename, qinfo["quality"])
            local_path = outdir_widgets / widget_filename

            # Crop from page via clip (consistent with your original)
            page.screenshot(path=str(local_path), clip={"x": px, "y": py, "width": pw_, "height": ph_})

            # Upload
            widget_key = f"{KDH_FOLDER_ROOT}/{session_folder}/widgets/{widget_filename}"
            uploaded_w = _storage_upload_bytes(KDH_BUCKET, widget_key, local_path.read_bytes())

            extraction_notes = {
                "quality": qinfo["quality"],
                "quality_score": qinfo["quality_score"],
                "quality_reason": qinfo["quality_reason"],
                "selector_kind": qinfo["selector_kind"],
                "title_present": qinfo["title_present"],
                "area_px": qinfo["area_px"],
                "threshold": QUALITY_THRESHOLD,
            }

            widget_row = {
                "widget_id": str(uuid.uuid4()),
                "url": url,
                "platform": platform,
                "report_name": report_name,
                "report_slug": report_slug,
                "widget_title": title,
                "widget_index": idx,
                "bbox": [px, py, pw_, ph_],
                "storage_path_widget": uploaded_w.get("key",""),
                "public_url_widget": uploaded_w.get("public_url",""),
                "captured_at": ts,
                "session_folder": session_folder,
                "extraction_notes": json.dumps(extraction_notes),
            }
            _db_insert(KDH_TABLE_WIDGETS, widget_row)

            widgets_saved.append({
                "idx": idx,
                "title": title,
                "bbox": [px,py,pw_,ph_],
                "local_path": str(local_path),
                "quality": qinfo["quality"],
                "quality_score": qinfo["quality_score"],
            })

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
        "full_path_local": str((Path("./screenshots")/session_folder/full_filename).resolve()),
        "storage_prefix": f"{KDH_FOLDER_ROOT}/{session_folder}/",
    }
