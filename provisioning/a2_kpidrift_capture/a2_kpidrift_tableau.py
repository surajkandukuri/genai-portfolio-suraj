from pathlib import Path
from typing import Dict

from playwright.sync_api import TimeoutError as PWTimeout
from .a2_kpidrift_engine import nowstamp, ensure_outdir, setup_logs, with_browser
from .a2_kpidrift_types import CaptureResult, Artifacts

# Optional selectors to try if there's no iframe
TABLEAU_CANDIDATES = "canvas, svg, div[role='img'], div.tabToolbar, div.tab-widget, div.tab-content"

@with_browser(headless=True)
def capture_tableau(ctx, url: str, outdir: Path) -> CaptureResult:
    outdir = ensure_outdir(outdir)
    ts = nowstamp()
    paths: Dict[str, Path] = {
        "full":   outdir / f"tableau_full_{ts}.png",
        "report": outdir / f"tableau_report_{ts}.png",
        "html":   outdir / f"tableau_page_{ts}.html",
        "log":    outdir / f"tableau_log_{ts}.txt",
    }

    page = ctx.new_page()
    logs = setup_logs(page)

    try:
        # Navigate & settle
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(6000)  # lightweight settle

        # Try to ensure iframe is present (many public Tableau embeds use one)
        try:
            ifr = page.locator("iframe").first
            if ifr.count():
                ifr.wait_for(timeout=20_000)
        except PWTimeout:
            pass

        # Top-left anchor for consistent full-page captures
        page.evaluate("window.scrollTo(0,0)")
        page.wait_for_timeout(800)

        # Full-page shot
        page.screenshot(path=str(paths["full"]), full_page=True)

        # Widget-sized shot (iframe first; otherwise best visual candidate)
        if page.locator("iframe").count():
            page.locator("iframe").first.screenshot(path=str(paths["report"]))
        else:
            cand = page.locator(TABLEAU_CANDIDATES).first
            cand.wait_for(timeout=8000)
            cand.screenshot(path=str(paths["report"]))

    except Exception:
        # Best-effort HTML dump for debugging
        try:
            paths["html"].write_text(page.content(), encoding="utf-8")
        except Exception:
            pass
    finally:
        if logs:
            paths["log"].write_text("\n".join(logs), encoding="utf-8")
        page.close()

    return CaptureResult(
        provider="tableau",
        url=url,
        outdir=outdir,
        artifacts=Artifacts(
            full=paths["full"], report=paths["report"],
            html=paths["html"],  log=paths["log"]
        ),
        meta={}
    )
