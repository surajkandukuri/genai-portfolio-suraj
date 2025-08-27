from pathlib import Path
from typing import Dict
from playwright.sync_api import TimeoutError as PWTimeout
from .a2_kpidrift_engine import nowstamp, ensure_outdir, setup_logs, with_browser
from .a2_kpidrift_types import CaptureResult, Artifacts

PBI_SELECTORS = [
    "div#pvExplorationHost",
    "div.canvasFlexBox",
    "div.reportCanvas",
    "div.visualContainerHost",
    "div[role='presentation'] canvas",
    "canvas"
]

@with_browser(headless=True)
def capture_powerbi(ctx, url: str, outdir: Path) -> CaptureResult:
    outdir = ensure_outdir(outdir)
    ts = nowstamp()
    paths: Dict[str, Path] = {
        "full":   outdir / f"powerbi_full_{ts}.png",
        "report": outdir / f"powerbi_report_{ts}.png",
        "html":   outdir / f"powerbi_page_{ts}.html",
        "log":    outdir / f"powerbi_log_{ts}.txt",
    }

    page = ctx.new_page()
    logs = setup_logs(page)

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(3500)
        page.locator("iframe").first.wait_for(timeout=20000)
        fl = page.frame_locator("iframe").first

        target = None
        for sel in PBI_SELECTORS:
            try:
                loc = fl.locator(sel).first
                loc.wait_for(timeout=8000)
                box = loc.bounding_box()
                if box and box["width"] > 300 and box["height"] > 200:
                    target = loc; break
            except PWTimeout:
                continue

        page.evaluate("window.scrollTo(0,0)")
        page.wait_for_timeout(800)
        page.screenshot(path=str(paths["full"]), full_page=True)

        if target:
            target.screenshot(path=str(paths["report"]))
        else:
            page.locator("iframe").first.screenshot(path=str(paths["report"]))

    except Exception:
        try: paths["html"].write_text(page.content(), encoding="utf-8")
        except Exception: pass
    finally:
        if logs:
            paths["log"].write_text("\n".join(logs), encoding="utf-8")
        page.close()

    return CaptureResult(
        provider="powerbi",
        url=url,
        outdir=outdir,
        artifacts=Artifacts(
            full=paths["full"], report=paths["report"],
            html=paths["html"],  log=paths["log"]
        ),
        meta={"selectors_tried": ",".join(PBI_SELECTORS)}
    )
