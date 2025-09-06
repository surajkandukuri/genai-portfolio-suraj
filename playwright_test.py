# playwright_test.py
# Full-page + per-widget capture for Power BI / Tableau (public links)
# Filenames: {platform}_{dashboardName}_{widgetTitle}_{widget#}.png
# Adds: Supabase upload + DB inserts to kdh_screengrab_dim / kdh_widget_dim
# Also keeps: debug dumps, report_override, and "largest-top-text" heuristic fallback.

from __future__ import annotations
import argparse, json, re, time, os, io, hashlib, mimetypes, uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PWTimeout,
    Page,
    Frame,
    Error as PWError,
)

# ───────────────────────────── Supabase setup ─────────────────────────────
# pip install supabase
try:
    from supabase import create_client, Client  # supabase>=2.x
except Exception:
    create_client = None
    Client = None

SUPABASE_BUCKET = os.environ.get("SUPABASE_BUCKET", "kpidrifthunter")
SUPABASE_PREFIX = os.environ.get("SUPABASE_PREFIX", "widgetextractor")

def _load_supabase() -> Optional[Client]:
    if create_client is None:
        print("⚠️  supabase-py not installed. Run: pip install supabase")
        return None
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("⚠️  SUPABASE_URL / SUPABASE_SERVICE_KEY not set; skipping upload & DB inserts.")
        return None
    try:
        return create_client(url, key)
    except Exception as e:
        print(f"⚠️  Failed to init Supabase client: {e}")
        return None

def _to_posix(path: str) -> str:
    return path.replace("\\", "/")

def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _bytes_and_type(path: Path) -> Tuple[bytes, str]:
    b = path.read_bytes()
    mt, _ = mimetypes.guess_type(str(path))
    return b, (mt or "application/octet-stream")

# ───────────────────────────── Utilities ─────────────────────────────
def _nowstamp() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _now_ts_tz() -> datetime:
    return datetime.now(timezone.utc)

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
    if "powerbi.com" in u:
        return "powerbi"
    if "tableau" in u:
        return "tableau"
    return "unknown"

def _stable_wait(page: Page, extra_ms: int = 500):
    try:
        page.wait_for_load_state("domcontentloaded", timeout=8000)
    except PWTimeout:
        pass
    page.wait_for_timeout(extra_ms)

# ───────────────────── Report name (dashboard) detector ─────────────────────
_BAD_EXACT = {
    "microsoft power bi", "power bi", "view report", "report",
    "dashboard", "sign in", "home", "sheet", "show filters",
    "navigating to visual", "use ctrl", "press ctrl", "press enter",
    "skip to report", "skip to main content"
}
_BAD_SUBSTR = [
    "navigating to visual", "use ctrl", "press ctrl", "keyboard shortcut",
    "skip to report", "skip to main content", "aria-live"
]

def _non_generic(txt: str) -> bool:
    low = (txt or "").strip().lower()
    if not low or low in _BAD_EXACT:
        return False
    if any(sub in low for sub in _BAD_SUBSTR):
        return False
    return True

def _sanitize_vendor_title(s: str) -> str:
    s = re.sub(r"\s*[-|]\s*Microsoft\s*Power\s*BI.*$", "", s, flags=re.I)
    s = re.sub(r"\s*-\s*Power\s*BI.*$", "", s, flags=re.I)
    s = re.sub(r"\s*-\s*Tableau.*$", "", s, flags=re.I)
    return s.strip()

def _pick_best_text(cands: List[str]) -> Optional[str]:
    scored = []
    for raw in cands:
        if not raw:
            continue
        t = raw.strip()
        if not _non_generic(t):
            continue
        score = len(t) + (3 if " " in t else 0)
        scored.append((score, t))
    if not scored:
        return None
    scored.sort(reverse=True)
    return scored[0][1]

def _guess_title_by_style(frame: Frame, top_px: int = 380) -> Optional[str]:
    """Heuristic: choose the largest, bold-ish text near the top of the report."""
    try:
        nodes = frame.evaluate(
            f"""(topLimit) => {{
              const take = [];
              const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
              while (walker.nextNode()) {{
                const el = walker.currentNode;
                if (!el) continue;
                const cs = window.getComputedStyle(el);
                if (cs.visibility === 'hidden' || cs.display === 'none' || parseFloat(cs.opacity || '1') < 0.1) continue;
                const rect = el.getBoundingClientRect();
                if (!rect || !rect.width || !rect.height) continue;
                if (rect.top > topLimit) continue;
                const text = (el.innerText || '').trim();
                if (!text || text.length < 4) continue;
                const size = parseFloat(cs.fontSize || '0');
                const weight = (cs.fontWeight || '').toString();
                take.push({{
                  text,
                  size: isNaN(size) ? 0 : size,
                  weight: /^(700|800|900|bold)$/i.test(weight) ? 1 : 0,
                  top: rect.top
                }});
              }}
              return take;
            }}""",
            top_px
        )
        if not nodes:
            return None

        def ok(t: str) -> bool:
            lt = t.lower()
            if lt in _BAD_EXACT: return False
            if "keyboard shortcut" in lt or "aria-live" in lt: return False
            return True

        scored = []
        for n in nodes:
            t = (n.get("text") or "").strip()
            if not t or not ok(t):
                continue
            size = float(n.get("size", 0))
            bold = int(n.get("weight", 0))
            top  = float(n.get("top", 9999.0))
            score = size*3 + bold*5 - top*0.01 + (3 if " " in t else 0)
            scored.append((score, t))

        if not scored:
            return None
        scored.sort(reverse=True)
        return scored[0][1]
    except Exception:
        return None

def _detect_report_name(page: Page, frame: Frame, *, debug=False) -> Tuple[str, Dict]:
    """
    Returns (name, debug_info).
    Priority:
      1) iframe@title / iframe@aria-label
      2) in-frame header titles
      3) ACTIVE page/tab label (aria-selected='true' or aria-current='page')
      4) meta og/twitter/<title> (sanitized)
      5) heuristic: largest bold-ish text near top of iframe
      6) URL last segment (avoid 'view')
    """
    candidates: List[Tuple[str, str]] = []
    dbg: Dict = {"iframe": {}, "headers": [], "active_tabs": [], "meta": {}, "picked": None, "heuristic": None}

    def add(src: str, val: Optional[str]):
        if not val:
            return
        t = (val or "").strip()
        if not t:
            return
        candidates.append((src, t))

    # 1) iframe attributes
    try:
        ifr = page.locator("iframe").first
        if ifr.count():
            t = ifr.get_attribute("title")
            a = ifr.get_attribute("aria-label")
            dbg["iframe"] = {"title": t, "aria-label": a}
            add("iframe@title", t)
            add("iframe@aria-label", a)
    except Exception:
        pass

    # 2) In-frame headers
    try:
        for sel in [
            "[data-testid='report-header-title']",
            "[data-testid='report-header'] [data-testid='title']",
            ".reportTitle", ".vcHeaderTitle",
        ]:
            loc = frame.locator(sel)
            n = min(8, loc.count())
            for i in range(n):
                try:
                    role = (loc.nth(i).get_attribute("role") or "").lower()
                    if "status" in role:
                        continue
                    txt = (loc.nth(i).inner_text() or "").strip()
                    dbg["headers"].append({"sel": sel, "text": txt, "role": role})
                    add(f"header:{sel}", txt)
                except Exception:
                    pass
    except Exception:
        pass

    # 3) ACTIVE tab/page only
    try:
        active_tab_sels = [
            "[aria-label='Pages Navigation'] [role='tab'][aria-selected='true']",
            "[role='tablist'] [role='tab'][aria-selected='true']",
            "[role='tab'][aria-current='page']",
            ".tab-toolbar .tab-title.active",
            ".tab-sheet-tab[aria-selected='true']",
        ]
        for sel in active_tab_sels:
            loc = frame.locator(sel)
            n = min(4, loc.count())
            for i in range(n):
                try:
                    txt = (loc.nth(i).inner_text() or "").strip()
                    dbg["active_tabs"].append({"sel": sel, "text": txt})
                    add(f"active-tab:{sel}", txt)
                except Exception:
                    pass
    except Exception:
        pass

    # 4) Meta titles and <title>, sanitized
    try:
        og = page.locator("meta[property='og:title']").first
        tw = page.locator("meta[name='twitter:title']").first
        pt = _sanitize_vendor_title(page.title() or "")
        if og and og.count():
            dbg["meta"]["og:title"] = _sanitize_vendor_title(og.get_attribute("content") or "")
            add("og:title", dbg["meta"]["og:title"])
        if tw and tw.count():
            dbg["meta"]["twitter:title"] = _sanitize_vendor_title(tw.get_attribute("content") or "")
            add("twitter:title", dbg["meta"]["twitter:title"])
        if pt:
            dbg["meta"]["<title>"] = pt
            add("<title>", pt)
    except Exception:
        pass

    # Choose best among explicit candidates
    best = _pick_best_text([t for _, t in candidates])
    if best:
        dbg["picked"] = {"source": "explicit", "text": best}
        return _sanitize_filename(best), dbg

    # 5) Heuristic: largest/ bold-ish top text inside iframe
    guess = _guess_title_by_style(frame, top_px=380)
    if guess and _non_generic(guess):
        dbg["heuristic"] = guess
        dbg["picked"] = {"source": "largest-top-text", "text": guess}
        return _sanitize_filename(guess), dbg

    # 6) URL fallback — avoid 'view'
    try:
        from urllib.parse import urlparse
        p = urlparse(page.url or "")
        seg = (p.path or "").rstrip("/").split("/")[-1]
        if (seg or "").lower() == "view":
            seg = ""
        name = _sanitize_filename(seg or (p.netloc or "report"))
        dbg["picked"] = {"source": "url-fallback", "text": name}
        return name, dbg
    except Exception:
        dbg["picked"] = {"source": "exception", "text": "report"}
        return "report", dbg

# ───────────────────── Widget detection & helpers ─────────────────────
BI_CANDIDATE_SELECTORS = [
    ".visualContainer, .visualContainerHost, .modernVisualOverlay",  # Power BI
    ".tab-worksheet, .tab-viz, .tabCanvas",                           # Tableau
    "[role='figure'], [role='img']",
    "svg, canvas",
]

def _pick_main_frame(page: Page, timeout_ms: int = 12000) -> Frame:
    start = time.time()
    best: Optional[Frame] = None
    best_score = -1
    while (time.time() - start) * 1000 < timeout_ms:
        for fr in page.frames:
            score = 0
            try:
                for sel in BI_CANDIDATE_SELECTORS:
                    score += fr.locator(sel).count()
            except PWError:
                pass
            if score > best_score:
                best_score = score
                best = fr
        if best_score > 0:
            return best or page.main_frame
        time.sleep(0.25)
    return best or page.main_frame

def _iou(a: Tuple[int,int,int,int], b: Tuple[int,int,int,int]) -> float:
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    x1, y1 = max(ax, bx), max(ay, by)
    x2, y2 = min(ax+aw, bx+bw), min(ay+ah, by+bh)
    inter = max(0, x2-x1) * max(0, y2-y1)
    if inter == 0:
        return 0.0
    ua = aw*ah + bw*bh - inter
    return inter / max(1, ua)

def _find_title_near(frame: Frame, box: Tuple[int,int,int,int]) -> Optional[str]:
    try:
        bx, by, bw, bh = box
        sel = ".visualTitle, .visualHeaderTitleText, [role='heading'], h1, h2, h3, h4, h5, h6"
        loc = frame.locator(sel)
        n = min(20, loc.count())
        closest = None
        best_dy = 99999
        for i in range(n):
            t = loc.nth(i)
            tb = t.bounding_box()
            if not tb:
                continue
            tx, ty, tw, th = int(tb["x"]), int(tb["y"]), int(tb["width"]), int(tb["height"])
            if ty < by and (tx < (bx + bw) and (tx + tw) > bx):
                dy = by - ty
                if 0 < dy < 220 and dy < best_dy:
                    label = (t.inner_text() or "").strip()
                    if label:
                        best_dy = dy
                        closest = label
        return closest
    except Exception:
        return None

@dataclass
class WidgetShot:
    idx: int
    title: str
    bbox: Tuple[int,int,int,int]
    path: str

# ───────────────────────────── Supabase I/O helpers ─────────────────────────────
def _upload_to_supabase(sb: Client, bucket: str, key: str, local_path: Path) -> bool:
    try:
        data, ctype = _bytes_and_type(local_path)
        # Supabase expects a bytes-like object
        res = sb.storage.from_(bucket).upload(_to_posix(key), data, {"contentType": ctype, "upsert": True})
        # supabase-py returns dict or raises; treat no exception as success
        return True
    except Exception as e:
        print(f"⚠️  Upload failed for {key}: {e}")
        return False

def _insert_row(sb: Client, table: str, payload: dict) -> Optional[dict]:
    try:
        res = sb.table(table).insert(payload).execute()
        return (res.data or [None])[0]
    except Exception as e:
        print(f"⚠️  Insert failed for {table}: {e} payload={payload}")
        return None

# ───────────────────────────── Core capture ─────────────────────────────
def capture_all(url: str, outdir: Path, viewport=(1920,1080), headed=False, max_widgets=60,
                report_override: Optional[str] = None, debug: bool = False,
                session_id: Optional[str] = None) -> Dict:
    outdir = _ensure_outdir(outdir)
    platform = _detect_platform(url)
    ts = _nowstamp()
    artifacts: Dict[str, str] = {}

    # session info
    capture_session_id = session_id or str(uuid.uuid4())
    session_slug = f"{capture_session_id}_{ts}"

    # Supabase client (optional)
    sb = _load_supabase()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not headed)
        ctx = browser.new_context(viewport={"width": viewport[0], "height": viewport[1]},
                                  device_scale_factor=2)
        page = ctx.new_page()

        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        _stable_wait(page, 1200)

        full_path = outdir / f"{platform}_full_{ts}.png"
        page.screenshot(path=str(full_path), full_page=True)
        artifacts["full"] = str(full_path.resolve())

        frame = _pick_main_frame(page, timeout_ms=12000)
        try:
            if frame == page.main_frame and page.locator("iframe").count():
                cf = page.locator("iframe").first.content_frame()
                if cf:
                    frame = cf
        except Exception:
            pass
        _stable_wait(page, 300)

        if report_override:
            report_name, dbg_info = _sanitize_filename(report_override), {"picked": {"source": "override", "text": report_override}}
        else:
            report_name, dbg_info = _detect_report_name(page, frame, debug=debug)
        artifacts["report_name"] = report_name

        if debug:
            try:
                (outdir / f"{platform}_{ts}_page_head.html").write_text(page.content(), encoding="utf-8")
                ifr = page.locator("iframe").first
                if ifr.count():
                    cf = ifr.content_frame()
                    if cf:
                        inner = cf.content()
                        (outdir / f"{platform}_{ts}_iframe.html").write_text(inner, encoding="utf-8")
            except Exception:
                pass
            (outdir / f"{platform}_{ts}_debug.json").write_text(json.dumps(dbg_info, indent=2), encoding="utf-8")
            print("[debug] name provenance:", json.dumps(dbg_info.get("picked", {}), indent=2))

        report_path = outdir / f"{platform}_{report_name}_report_{ts}.png"
        try:
            if page.locator("iframe").count():
                page.locator("iframe").first.screenshot(path=str(report_path))
            else:
                cand = page.locator("canvas, svg, [role='img']").first
                cand.wait_for(timeout=6000)
                cand.screenshot(path=str(report_path))
        except Exception:
            page.screenshot(path=str(report_path))
        artifacts["report"] = str(report_path.resolve())

        # ───── Upload full & insert screengrab row
        now_tz = _now_ts_tz()
        detected_via = (dbg_info.get("picked") or {}).get("source", "url")
        full_hash = _sha256_file(full_path)

        # storage keys (posix)
        base_prefix = f"{SUPABASE_PREFIX}/{session_slug}/{platform}_{report_name}"
        full_key = f"{base_prefix}/full/{Path(report_path).name}"
        full_key = _to_posix(full_key)

        if sb:
            _upload_to_supabase(sb, SUPABASE_BUCKET, full_key, Path(report_path))

        sg_row = {
            "capture_session_id": capture_session_id,
            "url": url,
            "platform": platform,
            "detected_via": detected_via,
            "platform_confidence": 1.000 if platform in ("powerbi", "tableau") else 0.000,
            "screengrab_hashvalue": full_hash,
            "storage_bucket": SUPABASE_BUCKET,
            "storage_path_full": full_key,                 # relative key in bucket
            "storage_filename": Path(report_path).name,    # bare filename
            "captured_at": now_tz.isoformat(),
        }
        screengrab_id: Optional[str] = None
        if sb:
            ins = _insert_row(sb, "kdh_screengrab_dim", sg_row)
            screengrab_id = ins and ins.get("screengrab_id")
        # If insert skipped (no supabase), keep screengrab_id None — crops still saved locally.

        # ───── Find widget-like elements
        candidates: List[Tuple[str, Tuple[int,int,int,int]]] = []
        MIN_W, MIN_H = 150, 100
        PAD = 12

        for sel in [
            ".visualContainer, .visualContainerHost, .modernVisualOverlay",
            ".tab-worksheet, .tab-viz, .tabCanvas",
            "[role='figure'], [role='img']",
            "svg, canvas",
        ]:
            try:
                loc = frame.locator(sel)
                n = min(40, loc.count())
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
                        candidates.append((sel, (x, y, w, h)))
                    except Exception:
                        pass
            except PWError:
                pass

        # Dedup and order TL->BR
        kept: List[Tuple[str, Tuple[int,int,int,int]]] = []
        for sel, box in sorted(candidates, key=lambda c: (c[1][1], c[1][0])):
            drop = False
            for _, kb in kept:
                if _iou(box, kb) > 0.72:
                    drop = True; break
            if not drop:
                kept.append((sel, box))

        widgets: List[WidgetShot] = []

        def pad_clip(x: int, y: int, w: int, h: int, p: int = PAD) -> Dict[str, int]:
            from math import floor
            return {"x": max(0, floor(x-p)), "y": max(0, floor(y-p)),
                    "width": max(1, floor(w+2*p)), "height": max(1, floor(h+2*p))}

        for idx, (_, (x, y, w, h)) in enumerate(kept[:max_widgets], start=1):
            try:
                page.evaluate(f"window.scrollTo(0, {max(y-200, 0)})")
                _stable_wait(page, 120)
            except Exception:
                pass

            title = _find_title_near(frame, (x, y, w, h)) or "Widget"
            title_stub = _sanitize_filename(title)
            filename = f"{platform}_{report_name}_{title_stub}_{idx:02d}.png"
            out_path = outdir / filename

            try:
                page.screenshot(path=str(out_path), clip=pad_clip(x, y, w, h))
                widgets.append(WidgetShot(idx=idx, title=title, bbox=(x, y, w, h), path=str(out_path.resolve())))
                print(f"✅ {idx:02d}  {filename}")
            except Exception as e:
                print(f"⚠️  Failed widget {idx}: {e}")
                continue

            # Upload crop & insert widget row
            widget_key = f"{base_prefix}/widgets/{filename}"
            widget_key = _to_posix(widget_key)
            if sb:
                _upload_to_supabase(sb, SUPABASE_BUCKET, widget_key, out_path)

            if sb and screengrab_id:
                w_row = {
                    "screengrab_id": screengrab_id,
                    "bbox_xywh": [x, y, w, h],
                    "storage_bucket": SUPABASE_BUCKET,
                    "storage_path_crop": widget_key,
                    "storage_filename": filename,
                    "extraction_stage": "captured",
                    "widget_title": title,
                    # useful bookkeeping (optional fields exist in your schema)
                    "widget_type": None,
                    "unit": None,
                    "agg": "unknown",
                    "ocr_confidence": None,
                    "classification_confidence": None,
                    "parsed_to_fact": False,
                }
                _insert_row(sb, "kdh_widget_dim", w_row)

        manifest = {
            "url": url,
            "platform": platform,
            "report_name": report_name,
            "captured_at": ts,
            "session_id": capture_session_id,
            "artifacts": artifacts,
            "name_debug": dbg_info if debug else None,
            "widgets": [asdict(w) for w in widgets],
            "storage_prefix": f"{SUPABASE_BUCKET}/{base_prefix}",
        }
        (outdir / f"{platform}_{report_name}_{_nowstamp()}.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        ctx.close(); browser.close()
        return manifest

# ───────────────────────────── CLI ─────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Full + per-widget capture (Power BI/Tableau public) + Supabase upload/insert.")
    ap.add_argument("--url", required=True, help="Dashboard URL.")
    ap.add_argument("--out", default="screenshots", help="Output folder (default: screenshots).")
    ap.add_argument("--viewport", default="1920x1080", help="Viewport WxH (e.g., 1920x1080).")
    ap.add_argument("--max", type=int, default=60, help="Max widgets to capture.")
    ap.add_argument("--headed", action="store_true", help="Show browser window.")
    ap.add_argument("--debug", action="store_true", help="Dump debug files and provenance.")
    ap.add_argument("--report_override", type=str, default=None, help="Force dashboard name.")
    ap.add_argument("--session-id", type=str, default=None, help="Optional UUID to group captures; auto if omitted.")
    args = ap.parse_args()

    try:
        w, h = map(int, args.viewport.lower().split("x"))
    except Exception:
        w, h = 1920, 1080

    outdir = Path(args.out)
    manifest = capture_all(
        url=args.url, outdir=outdir, viewport=(w, h),
        headed=args.headed, max_widgets=args.max,
        report_override=args.report_override, debug=args.debug,
        session_id=args.session_id
    )
    print("\nSummary:", json.dumps({k: v for k, v in manifest.items() if k not in ('widgets','name_debug')}, indent=2))
    if args.debug:
        print("\nName provenance:", json.dumps(manifest.get("name_debug", {}), indent=2))
        print("\nStorage prefix:", manifest.get("storage_prefix"))

if __name__ == "__main__":
    main()
