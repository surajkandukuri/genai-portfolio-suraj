# capture_dashboards.py
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page, Locator

# Defaults
DEFAULT_PBI = (
    "https://app.powerbi.com/view?"
    "r=eyJrIjoiNWU3OTQxZTItMWFiMi00NWE4LTk5NGQtYjllMjc1ODFjNjlhIiwidCI6Ijg5YTg4Mjgw"
    "LTFhMDQtNGNlZi05NWQ5LWE3YTI1NTYyMzc4ZCJ9"
)
DEFAULT_TBL = "https://public.tableau.com/app/profile/citam/viz/OlistDashboard_16494074594040/OlistOrdersOverview"

# ── Utilities ──────────────────────────────────────────────────────────────────
def _nowstamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def _setup_logs(page: Page):
    logs = []
    # NOTE: properties, not callables
    page.on("console",       lambda m: logs.append(f"[console:{m.type}] {m.text}"))
    page.on("pageerror",     lambda e: logs.append(f"[pageerror] {e}"))
    page.on("requestfailed", lambda r: logs.append(f"[requestfailed] {r.url} :: {getattr(r, 'failure', None)}"))
    return logs

def _ensure_outdir(outdir: Path) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir

def _save_logs(outdir: Path, logs, stem: str) -> Optional[Path]:
    if not logs: return None
    p = outdir / f"{stem}_log.txt"
    p.write_text("\n".join(map(str, logs)), encoding="utf-8")
    return p

def _artifact_paths(outdir: Path, stem: str) -> Dict[str, Path]:
    ts = _nowstamp()
    return {
        "full":  outdir / f"{stem}_full_{ts}.png",
        "report":outdir / f"{stem}_report_{ts}.png",
        "html":  outdir / f"{stem}_page_{ts}.html",
    }

def _first_iframe(page: Page) -> Locator:
    return page.locator("iframe").first

def _wait_render_in_frame(page: Page, timeout_ms: int = 20000) -> Tuple[bool,str]:
    """
    Try to detect that visuals are painted by waiting for common selectors.
    Returns (ok, where) where 'where' is a short note for logging.
    """
    try:
        if page.locator("iframe").count():
            fl = page.frame_locator("iframe").first
            fl.locator("body").wait_for(timeout=timeout_ms)
            try:
                fl.locator("canvas, svg").first.wait_for(timeout=6000)
            except PWTimeout:
                pass
            return True, "frame"
        else:
            page.locator("body").wait_for(timeout=timeout_ms)
            try:
                page.locator("canvas, svg").first.wait_for(timeout=6000)
            except PWTimeout:
                pass
            return True, "main"
    except PWTimeout:
        return False, "timeout"

def _scroll_top(page: Page):
    try:
        page.evaluate("window.scrollTo(0,0)")
    except Exception:
        pass

# ── Providers ──────────────────────────────────────────────────────────────────
def capture_powerbi(url: str, outdir: Path) -> Dict[str, Path]:
    """
    Power BI public capture — FIXED:
      - Avoid networkidle (flaky). Use domcontentloaded + explicit waits.
      - Wait for a *real* canvas/host inside the iframe and ensure it has size.
      - Screenshot full page after render (prevents white PNGs).
      - Screenshot the specific report element for a tight crop (prevents tall/skinny shots).
    """
    outdir = _ensure_outdir(outdir)
    paths = _artifact_paths(outdir, "powerbi")

    # Selectors seen in public PBI "view" pages
    PBI_SELECTORS = [
        "div#pvExplorationHost",                # primary host
        "div.canvasFlexBox",                    # canvas wrapper
        "div.reportCanvas",                     # older host name
        "div.visualContainerHost",              # grid of visuals
        "div[role='presentation'] canvas",      # direct canvas
        "canvas"                                # absolute fallback
    ]

    with sync_playwright() as pw:
        # Set headless=False, slow_mo=300 to observe, if needed
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1920, "height": 1080}, device_scale_factor=2)
        page = ctx.new_page()
        logs = _setup_logs(page)

        try:
            # Load & let PBI hydrate
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(3500)

            # Wait for the iframe to appear
            page.locator("iframe").first.wait_for(timeout=20000)
            fl = page.frame_locator("iframe").first

            # Find the first working selector inside the frame and ensure it has size
            target = None
            for sel in PBI_SELECTORS:
                try:
                    loc = fl.locator(sel).first
                    loc.wait_for(timeout=8000)
                    # ensure it's visible and has area
                    box = loc.bounding_box()
                    if box and box["width"] > 300 and box["height"] > 200:
                        target = loc
                        break
                except PWTimeout:
                    continue

            if not target:
                # If none matched, wait a bit more for any canvas and try again once
                page.wait_for_timeout(3000)
                for sel in PBI_SELECTORS:
                    try:
                        loc = fl.locator(sel).first
                        loc.wait_for(timeout=5000)
                        box = loc.bounding_box()
                        if box and box["width"] > 300 and box["height"] > 200:
                            target = loc
                            break
                    except PWTimeout:
                        continue

            # FULL PAGE after render (prevents white)
            _scroll_top(page)
            page.wait_for_timeout(800)  # tiny settle
            page.screenshot(path=str(paths["full"]), full_page=True)
            print("✅ PBI full page:", paths["full"].resolve())

            # REPORT CROP
            if target:
                target.screenshot(path=str(paths["report"]))
                print("✅ PBI report (tight element):", paths["report"].resolve())
            else:
                # last resort
                _first_iframe(page).screenshot(path=str(paths["report"]))
                print("⚠️  PBI fallback (iframe element):", paths["report"].resolve())

        except Exception as e:
            print("❌ PBI error:", repr(e))
            try:
                paths["html"].write_text(page.content(), encoding="utf-8")
                print("🧾 PBI HTML snapshot:", paths["html"].resolve())
            except Exception:
                pass
        finally:
            lp = _save_logs(outdir, logs, "powerbi")
            if lp: print("📝 PBI log:", lp.resolve())
            ctx.close(); browser.close()

    return paths

def capture_tableau(url: str, outdir: Path) -> Dict[str, Path]:
    """
    Tableau Public capture.
    Strategy:
      1) full-page screenshot
      2) locate first iframe and capture its body
      3) if multiple iframes, fall back to the one that has content
      4) final fallback: viewport
    """
    outdir = _ensure_outdir(outdir)
    paths = _artifact_paths(outdir, "tableau")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)  # headless=False to watch it
        ctx = browser.new_context(viewport={"width": 1920, "height": 1080}, device_scale_factor=2)
        page = ctx.new_page()
        logs = _setup_logs(page)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(6000)  # Tableau paints a bit slower
            ok, where = _wait_render_in_frame(page, 25000)
            print(f"ℹ️  Tableau render wait: {ok} ({where})")

            _scroll_top(page)
            page.screenshot(path=str(paths["full"]), full_page=True)
            print("✅ Tableau full page:", paths["full"].resolve())

            # Tight crop of the viz
            try:
                if page.locator("iframe").count():
                    _first_iframe(page).screenshot(path=str(paths["report"]))
                    print("✅ Tableau report (iframe element):", paths["report"].resolve())
                else:
                    # Rare but handle: direct render
                    cand = page.locator("canvas, svg, div[role='img']").first
                    cand.wait_for(timeout=8000)
                    cand.screenshot(path=str(paths["report"]))
                    print("✅ Tableau report (main DOM):", paths["report"].resolve())
            except PWTimeout:
                page.screenshot(path=str(paths["report"]), full_page=False)
                print("⚠️  Tableau fallback viewport:", paths["report"].resolve())

        except Exception as e:
            print("❌ Tableau error:", repr(e))
            try:
                paths["html"].write_text(page.content(), encoding="utf-8")
                print("🧾 Tableau HTML snapshot:", paths["html"].resolve())
            except Exception:
                pass
        finally:
            lp = _save_logs(outdir, logs, "tableau")
            if lp: print("📝 Tableau log:", lp.resolve())
            ctx.close(); browser.close()

    return paths

# ── CLI ────────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Capture screenshots for Power BI or Tableau public dashboards.")
    ap.add_argument("--provider", choices=["powerbi", "tableau"], required=True, help="Which dashboard provider to capture.")
    ap.add_argument("--url", type=str, help="Dashboard URL. If omitted, uses a sensible default.")
    ap.add_argument("--outdir", type=str, default="screenshots", help="Output directory (default: screenshots).")
    args = ap.parse_args()

    url = args.url or (DEFAULT_PBI if args.provider == "powerbi" else DEFAULT_TBL)
    outdir = Path(args.outdir)

    print("Provider:", args.provider)
    print("URL     :", url)
    print("Outdir  :", outdir.resolve())

    if args.provider == "powerbi":
        capture_powerbi(url, outdir)
    else:
        capture_tableau(url, outdir)

if __name__ == "__main__":
    main()
