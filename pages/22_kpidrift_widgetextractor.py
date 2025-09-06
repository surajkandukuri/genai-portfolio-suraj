# pages/22_kpidrift_widgetextractor.py
# Streamlit page that extracts widget crops EXACTLY like playwright_test.py:
# - crops via Playwright page.screenshot(clip=...) from the live DOM
# - filenames: {platform}_{reportName}_{widgetTitle}_{idx}.png
# - saves locally to ./screenshots/{session}/{slug}/widgets/ (no Supabase writes yet)

from __future__ import annotations

# ── Standard imports
import os, re, io, time, json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import streamlit as st
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Error as PWError

# ──────────────────────────────────────────────────────────────────────────────
# If another page already calls set_page_config(), comment the next line out.
st.set_page_config(page_title="KPI Drift — Widget Extractor", page_icon="🧩", layout="wide")
st.title("🧩 KPI Drift — Widget Extractor")
st.caption("Select screengrabs and extract per-chart crops. This build saves to ./screenshots locally for verification.")

# ========== Small helpers (same spirit as the CLI) ==========
def _nowstamp() -> str:
    from datetime import datetime
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _ensure_outdir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def _sanitize_filename(s: str, max_len: int = 100) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-. ]+", "_", s)
    s = re.sub(r"\s+", "_", s)
    return (s[:max_len] or "untitled").rstrip("._-")

def _detect_platform(url: str) -> str:
    u = (url or "").lower()
    if "powerbi.com" in u: return "powerbi"
    if "tableau" in u:     return "tableau"
    return "unknown"

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
    s = re.sub(r"\s*-\s*Tableau.*$", "", s, flags=re.I)
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
    """Return (report_name, debug_info) using the same priority as CLI + heuristic."""
    candidates: List[Tuple[str, str]] = []
    dbg: Dict = {"iframe": {}, "headers": [], "active_tabs": [], "meta": {}, "picked": None, "heuristic": None}
    def add(src: str, val: Optional[str]):
        if not val: return
        t = (val or "").strip()
        if not t: return
        candidates.append((src, t))

    # iframe title / aria-label
    try:
        ifr = page.locator("iframe").first
        if ifr.count():
            t = ifr.get_attribute("title"); a = ifr.get_attribute("aria-label")
            dbg["iframe"] = {"title": t, "aria-label": a}
            add("iframe@title", t); add("iframe@aria-label", a)
    except Exception: pass

    # in-frame header titles
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

    # ACTIVE tab only
    try:
        for sel in ["[aria-label='Pages Navigation'] [role='tab'][aria-selected='true']",
                    "[role='tablist'] [role='tab'][aria-selected='true']",
                    "[role='tab'][aria-current='page']",
                    ".tab-toolbar .tab-title.active",
                    ".tab-sheet-tab[aria-selected='true']"]:
            loc = frame.locator(sel)
            n = min(4, loc.count())
            for i in range(n):
                try:
                    txt = (loc.nth(i).inner_text() or "").strip()
                    dbg["active_tabs"].append({"sel": sel, "text": txt})
                    add(f"active-tab:{sel}", txt)
                except Exception: pass
    except Exception: pass

    # page/meta titles
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

    # heuristic: largest top text
    guess = _guess_title_by_style(frame, top_px=380)
    if guess and _non_generic(guess):
        dbg["heuristic"] = guess
        dbg["picked"]   = {"source":"largest-top-text","text":guess}
        return _sanitize_filename(guess), dbg

    # fallback: URL segment (avoid 'view')
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

def _iou(a: Tuple[int,int,int,int], b: Tuple[int,int,int,int]) -> float:
    ax, ay, aw, ah = a; bx, by, bw, bh = b
    x1, y1 = max(ax, bx), max(ay, by)
    x2, y2 = min(ax+aw, bx+bw), min(ay+ah, by+bh)
    inter = max(0, x2-x1) * max(0, y2-y1)
    if inter == 0: return 0.0
    ua = aw*ah + bw*bh - inter
    return inter / max(1, ua)

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

# ========== load screengrabs list from DB (read-only) ==========
# If you want a pure-URL input instead, uncomment the small "Manual URL" block later.

from supabase import create_client
def _sget(*keys, default=None):
    for k in keys:
        try:
            if k in st.secrets: return st.secrets[k]
        except Exception:
            pass
        v = os.getenv(k)
        if v: return v
    return default

SUPABASE_URL = _sget("SUPABASE_URL","SUPABASE__URL")
SUPABASE_KEY = _sget("SUPABASE_SERVICE_ROLE_KEY","SUPABASE_SERVICE_KEY","SUPABASE_ANON_KEY","SUPABASE__SUPABASE_SERVICE_KEY")
KDH_BUCKET   = _sget("KDH_BUCKET", default="kpidrifthunter")

sb = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        st.warning(f"Supabase not available ({e}); you can still paste a URL below.")

@st.cache_data(ttl=60)
def load_recent_screengrabs(limit=200):
    if not sb: return []
    q = (
        sb.table("kdh_screengrab_dim")
          .select("screengrab_id, url, platform, storage_path_full, captured_at")
          .order("captured_at", desc=True)
          .limit(limit)
          .execute()
    )
    return q.data or []

rows = load_recent_screengrabs()
df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["screengrab_id","url","platform","captured_at","storage_path_full"])
df["Select"] = False

st.subheader("Recent Screengrabs (from DB)")
edited = st.data_editor(
    df[["Select", "url", "platform", "captured_at", "screengrab_id", "storage_path_full"]],
    column_config={
        "Select": st.column_config.CheckboxColumn("Select"),
        "url": st.column_config.LinkColumn("URL"),
        "platform": "Platform",
        "captured_at": "Captured At (UTC)",
        "screengrab_id": st.column_config.TextColumn("ID"),
        "storage_path_full": st.column_config.TextColumn("Storage Path"),
    },
    use_container_width=True,
    hide_index=True,
    num_rows="dynamic",
    height=420,
)
selected = edited[edited["Select"] == True]  # noqa: E712

st.markdown("— or —")
manual_url = st.text_input("Manual URL (Power BI / Tableau public link)")

c1, c2, _ = st.columns([1,1,6])
go_btn = c1.button("▶️ Extract to ./screenshots", type="primary", use_container_width=True,
                   disabled=(selected.empty and not manual_url.strip()))
refresh_btn = c2.button("↻ Refresh DB list", use_container_width=True)
if refresh_btn:
    st.cache_data.clear()
    st.rerun()

# ========== Core extraction (DOM clip screenshots) ==========
def extract_like_cli(url: str, outdir: Path, viewport=(1920,1080), scale=2.0, max_widgets=60) -> Dict:
    outdir = _ensure_outdir(outdir)
    platform = _detect_platform(url)
    ts = _nowstamp()
    artifacts: Dict[str, str] = {}

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

        # Full page screenshot (like CLI)
        full_path = outdir / f"{platform}_full_{ts}.png"
        page.screenshot(path=str(full_path), full_page=True)
        artifacts["full"] = str(full_path.resolve())

        # Pick content frame if present
        frame = page.main_frame
        try:
            if page.locator("iframe").count():
                cf = page.locator("iframe").first.content_frame()
                if cf: frame = cf
        except Exception:
            pass

        # Report name detection
        report_name, _dbg = _detect_report_name(page, frame)

        # Candidate elements
        selectors = [
            ".visualContainer, .visualContainerHost, .modernVisualOverlay",   # Power BI
            ".tab-worksheet, .tab-viz, .tabCanvas",                            # Tableau
            "[role='figure'], [role='img']",
            "svg, canvas",
        ]
        MIN_W, MIN_H = 150, 100
        PAD = 12
        candidates: List[Tuple[str, Tuple[int,int,int,int]]] = []

        for sel in selectors:
            try:
                loc = frame.locator(sel)
                n = min(40, loc.count())
                for i in range(n):
                    el = loc.nth(i)
                    try:
                        bb = el.bounding_box()
                        if not bb: continue
                        x, y = int(bb["x"]), int(bb["y"]); w, h = int(bb["width"]), int(bb["height"])
                        if w < MIN_W or h < MIN_H: continue
                        candidates.append((sel, (x, y, w, h)))
                    except Exception: pass
            except PWError:
                pass

        kept: List[Tuple[str, Tuple[int,int,int,int]]] = []
        for sel, box in sorted(candidates, key=lambda c: (c[1][1], c[1][0])):  # TL->BR
            drop = False
            for _, kb in kept:
                if _iou(box, kb) > 0.72:
                    drop = True; break
            if not drop:
                kept.append((sel, box))

        # Save crops with Playwright CLIP (like CLI)
        saved = []
        for idx, (_, (x, y, w, h)) in enumerate(kept[:max_widgets], start=1):
            # small pad
            px, py = max(0, x-PAD), max(0, y-PAD)
            pw, ph = max(1, w+2*PAD), max(1, h+2*PAD)

            # Try to infer a title near the top of the box for naming
            title = _find_title_near(frame, (x, y, w, h)) or "Widget"
            title_stub = _sanitize_filename(title)
            filename = f"{platform}_{report_name}_{title_stub}_{idx:02d}.png"

            out_path = outdir / filename
            try:
                page.screenshot(path=str(out_path), clip={"x": px, "y": py, "width": pw, "height": ph})
                saved.append({"idx":idx,"title":title,"bbox":[px,py,pw,ph],"path":str(out_path.resolve())})
                st.write(f"✅ {idx:02d}  {filename}")
            except Exception as e:
                st.write(f"⚠️ Failed {idx}: {e}")

        ctx.close(); browser.close()

        return {
            "url": url,
            "platform": platform,
            "report_name": report_name,
            "captured_at": ts,
            "artifacts": artifacts,
            "widgets": saved,
        }

# ========== Run ==========
if go_btn:
    targets: List[Tuple[str,str]] = []  # (url, suggested_subdir)

    if not selected.empty:
        for _, r in selected.iterrows():
            url = r["url"]
            # local folder under ./screenshots/{platform}/{ts_or_slug}
            plat = _detect_platform(url)
            sub = f"{plat}_{_nowstamp()}"
            targets.append((url, sub))

    if manual_url.strip():
        plat = _detect_platform(manual_url.strip())
        targets.append((manual_url.strip(), f"{plat}_{_nowstamp()}"))

    if not targets:
        st.warning("No rows selected and no manual URL provided.")
    else:
        base = _ensure_outdir(Path("./screenshots"))
        for url, sub in targets:
            st.subheader(f"Extracting → {url}")
            outdir = _ensure_outdir(base / sub / "widgets")
            manifest = extract_like_cli(url=url, outdir=outdir, viewport=(1920,1080), scale=2.0)
            # Save a small manifest JSON per run
            (outdir.parent / f"manifest_{_nowstamp()}.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            st.success(f"Saved {len(manifest['widgets'])} crops to: {outdir}")
            st.code(str(outdir), language="text")

# (No auto-rerun here; the page stays open so you can inspect results.)
