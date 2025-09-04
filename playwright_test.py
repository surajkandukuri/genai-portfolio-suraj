# playwright_test.py
# Full-page capture + per-widget crops with KPI Drift naming:
# {platform}_{dashboardName}_{widgetTitle}_{widget#}.png
# Works for Power BI public "view" links and Tableau Public.

from __future__ import annotations
import argparse, json, re, time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PWTimeout,
    Page,
    Frame,
    Error as PWError,
)

# -----------------------------
# Utilities
# -----------------------------
def _nowstamp() -> str:
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
    if "powerbi.com" in u:
        return "powerbi"
    if "tableau" in u:
        return "tableau"
    return "unknown"

def _pick_best_text(cands: List[str]) -> Optional[str]:
    # Filter obvious boilerplate; score by length with slight bonus for multi-word
    bad_exact = {"power bi", "tableau", "sign in", "home", "sheet", "dashboard", "show filters"}
    scored = []
    for raw in cands:
        if not raw:
            continue
        t = raw.strip()
        low = t.lower()
        if low in bad_exact:
            continue
        score = len(t) + (3 if " " in t else 0)
        scored.append((score, t))
    if not scored:
        return None
    scored.sort(reverse=True)
    return scored[0][1]

def _detect_report_name(page: Page, frame: Frame) -> str:
    """Try hard to get a human title for Power BI / Tableau.
    Order: in-frame header → page-tabs → <title>/og:title → URL segment (avoid 'view').
    """
    cands: List[str] = []

    # 1) Power BI header/title spots (inside the content frame)
    try:
        pbi_header_selectors = [
            "[data-testid='report-header-title']",
            "[data-testid='report-header'] [data-testid='title']",
            ".visualHeaderTitleText", ".reportToolbar", ".vcHeaderTitle", ".logoArea .title",
            "[aria-label*='Report title']", "[aria-label*='Dashboard title']",
        ]
        for sel in pbi_header_selectors:
            loc = frame.locator(sel)
            n = min(8, loc.count())
            for i in range(n):
                try:
                    txt = (loc.nth(i).inner_text() or "").strip()
                    if txt:
                        cands.append(txt)
                except Exception:
                    pass
    except Exception:
        pass

    # 2) Power BI page/tab bar (bottom tabs often carry good names)
    try:
        tab_sels = [
            "[aria-label='Pages Navigation'] [role='tab']",
            "[role='tablist'] [role='tab']",
            ".navigationBarCanvas [role='tab']",
        ]
        for sel in tab_sels:
            loc = frame.locator(sel)
            n = min(8, loc.count())
            for i in range(n):
                try:
                    txt = (loc.nth(i).inner_text() or "").strip()
                    if txt:
                        cands.append(txt)
                except Exception:
                    pass
    except Exception:
        pass

    # 3) Tableau toolbar / sheet titles (if Tableau)
    try:
        tbl_sels = [".tab-toolbar .tab-title", ".tabLabel", ".tab-sheet-tab", ".tab-worksheet"]
        for sel in tbl_sels:
            loc = frame.locator(sel)
            n = min(8, loc.count())
            for i in range(n):
                try:
                    txt = (loc.nth(i).inner_text() or "").strip()
                    if txt:
                        cands.append(txt)
                except Exception:
                    pass
    except Exception:
        pass

    # 4) Browser <title> and og:title (sanitize)
    try:
        t = page.title()
        if t:
            t = re.sub(r"\s*-\s*Power BI.*$", "", t, flags=re.I)
            t = re.sub(r"\s*-\s*Tableau.*$", "", t, flags=re.I)
            if t.strip():
                cands.append(t.strip())
    except Exception:
        pass
    try:
        og = page.locator("meta[property='og:title']").first
        if og:
            ogc = og.get_attribute("content")
            if ogc:
                ogc = re.sub(r"\s*-\s*Power BI.*$", "", ogc, flags=re.I)
                ogc = re.sub(r"\s*-\s*Tableau.*$", "", ogc, flags=re.I)
                cands.append((ogc or "").strip())
    except Exception:
        pass

    best = _pick_best_text(cands)
    if best:
        return best

    # 5) URL fallback — avoid 'view'
    try:
        from urllib.parse import urlparse
        p = urlparse(page.url or "")
        seg = (p.path or "").rstrip("/").split("/")[-1] or ""
        seg = seg if seg.lower() != "view" else ""
        return seg or (p.netloc or "report")
    except Exception:
        return "report"

# -----------------------------
# Widget detection
# -----------------------------
BI_CANDIDATE_SELECTORS = [
    # Containers first (usually include titles/axes)
    ".visualContainer, .visualContainerHost, .modernVisualOverlay",  # Power BI
    ".tab-worksheet, .tab-viz, .tabCanvas",                           # Tableau
    # A11y wrappers
    "[role='figure'], [role='img']",
    # Drawing primitives
    "svg, canvas",
]

def _stable_wait(page: Page, extra_ms: int = 500):
    try:
        page.wait_for_load_state("domcontentloaded", timeout=8000)
    except PWTimeout:
        pass
    page.wait_for_timeout(extra_ms)

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
    # Search above the box for headings likely to be the visual title
    try:
        bx, by, bw, bh = box
        sel = ".visualTitle, .visualHeaderTitleText, [role='heading'], h1, h2, h3, h4, h5, h6"
        loc = frame.locator(sel)
        n = min(12, loc.count())
        closest = None
        best_dy = 99999
        for i in range(n):
            t = loc.nth(i)
            tb = t.bounding_box()
            if not tb:
                continue
            tx, ty, tw, th = int(tb["x"]), int(tb["y"]), int(tb["width"]), int(tb["height"])
            if ty < by and (tx < (bx + bw) and (tx + tw) > bx):  # above & horizontally overlapping
                dy = by - ty
                if 0 < dy < 200 and dy < best_dy:
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

# -----------------------------
# Core capture
# -----------------------------
def capture_all(url: str, outdir: Path, viewport=(1920,1080), headed=False, max_widgets=60) -> Dict:
    outdir = _ensure_outdir(outdir)
    platform = _detect_platform(url)
    ts = _nowstamp()
    artifacts = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not headed)
        ctx = browser.new_context(viewport={"width": viewport[0], "height": viewport[1]},
                                  device_scale_factor=2)
        page = ctx.new_page()

        # ---------- Load ----------
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        _stable_wait(page, 1000)

        # Full-page screenshot
        full_path = outdir / f"{platform}_full_{ts}.png"
        page.screenshot(path=str(full_path), full_page=True)
        artifacts["full"] = str(full_path.resolve())

        # Pick viz frame + detect report name
        frame = _pick_main_frame(page, timeout_ms=12000)
        _stable_wait(page, 300)
        report_name = _sanitize_filename(_detect_report_name(page, frame))
        artifacts["report_name"] = report_name

        # Tight crop of the report (iframe or largest viz container)
        report_path = outdir / f"{platform}_{report_name}_report_{ts}.png"
        try:
            if page.locator("iframe").count():
                page.locator("iframe").first.screenshot(path=str(report_path))
            else:
                cand = page.locator("canvas, svg, [role='img']").first
                cand.wait_for(timeout=6000)
                cand.screenshot(path=str(report_path))
        except Exception:
            # fallback to viewport
            page.screenshot(path=str(report_path))
        artifacts["report"] = str(report_path.resolve())

        # ---------- Find widgets ----------
        candidates: List[Tuple[str, Tuple[int,int,int,int]]] = []
        MIN_W, MIN_H = 150, 100
        PAD = 12

        for sel in BI_CANDIDATE_SELECTORS:
            try:
                loc = frame.locator(sel)
                n = min(30, loc.count())
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

        # Dedup (IoU) and sort top-left -> bottom-right
        kept: List[Tuple[str, Tuple[int,int,int,int]]] = []
        for sel, box in sorted(candidates, key=lambda c: (c[1][1], c[1][0])):
            drop = False
            for _, kb in kept:
                if _iou(box, kb) > 0.72:
                    drop = True; break
            if not drop:
                kept.append((sel, box))

        # ---------- Save widget crops ----------
        widgets: List[WidgetShot] = []

        def pad_clip(x,y,w,h,p=PAD):
            # We'll scroll so the target is in the viewport, then clip within viewport
            from math import floor
            return {"x": max(0, floor(x-p)), "y": max(0, floor(y-p)),
                    "width": max(1, floor(w+2*p)), "height": max(1, floor(h+2*p))}

        for idx, (_, (x, y, w, h)) in enumerate(kept[:max_widgets], start=1):
            # Scroll to bring the box into view before clipping the page
            try:
                page.evaluate(f"window.scrollTo(0, {max(y-200, 0)})")
                _stable_wait(page, 150)
            except Exception:
                pass

            title = _find_title_near(frame, (x,y,w,h)) or "Widget"
            title_stub = _sanitize_filename(title)
            filename = f"{platform}_{report_name}_{title_stub}_{idx:02d}.png"
            out_path = outdir / filename

            try:
                clip = pad_clip(x,y,w,h)
                page.screenshot(path=str(out_path), clip=clip)
                widgets.append(WidgetShot(idx=idx, title=title, bbox=(x,y,w,h), path=str(out_path.resolve())))
                print(f"✅ {idx:02d}  {filename}")
            except Exception as e:
                print(f"⚠️  Failed widget {idx}: {e}")

        # Manifest (optional)
        manifest = {
            "url": url,
            "platform": platform,
            "report_name": report_name,
            "captured_at": ts,
            "artifacts": artifacts,
            "widgets": [asdict(w) for w in widgets],
        }
        (outdir / f"{platform}_{report_name}_{ts}.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        ctx.close(); browser.close()
        return manifest

# -----------------------------
# CLI
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Full + per-widget capture for Power BI/Tableau with KPI Drift naming.")
    ap.add_argument("--url", required=True, help="Dashboard URL (Power BI or Tableau).")
    ap.add_argument("--out", default="screenshots", help="Output folder.")
    ap.add_argument("--viewport", default="1920x1080", help="Viewport WxH, e.g. 1920x1080")
    ap.add_argument("--max", type=int, default=60, help="Max widgets to capture.")
    ap.add_argument("--headed", action="store_true", help="Run with a visible browser.")
    args = ap.parse_args()

    try:
        w, h = map(int, args.viewport.lower().split("x"))
    except Exception:
        w, h = 1920, 1080

    outdir = Path(args.out)
    manifest = capture_all(url=args.url, outdir=outdir, viewport=(w,h), headed=args.headed, max_widgets=args.max)
    print("\nSummary:", json.dumps({k:v for k,v in manifest.items() if k != "widgets"}, indent=2))



if __name__ == "__main__":
    main()
