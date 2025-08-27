# provisioning/a2_kpidrift_capture/a2_kpidrift_engine.py

# --- MUST RUN BEFORE PLAYWRIGHT IS IMPORTED (fixes Windows asyncio subprocess) ---
import sys, asyncio
import platform

print(type(asyncio.get_event_loop_policy()).__name__)

if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from datetime import datetime
from pathlib import Path
from typing import List, Callable, Any, Tuple, Optional, Dict, Union

from playwright.sync_api import sync_playwright, Page

# ─────────────────────────── Utilities ───────────────────────────

def nowstamp() -> str:
    """Return a YYYYMMDD_HHMMSS timestamp string."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_outdir(p: Path) -> Path:
    """mkdir -p for output directories; return the same Path."""
    p.mkdir(parents=True, exist_ok=True)
    return p


def setup_logs(page: Page) -> List[str]:
    """Attach console/pageerror/requestfailed listeners and collect logs."""
    logs: List[str] = []
    page.on("console",       lambda m: logs.append(f"[console:{m.type}] {m.text}"))
    page.on("pageerror",     lambda e: logs.append(f"[pageerror] {e}"))
    page.on("requestfailed", lambda r: logs.append(f"[requestfailed] {r.url} :: {getattr(r, 'failure', None)}"))
    return logs


# ─────────────────────── Header sanitation ───────────────────────

HeaderVal = Union[str, bytes, bool, int, float, None]
HeadersType = Optional[Dict[str, HeaderVal]]

def clean_headers(h: HeadersType) -> Dict[str, Union[str, bytes]]:
    """
    Playwright/requests require header values to be str or bytes.
    - Drop None
    - Convert bool -> "1"/"0" (useful for DNT, Upgrade-Insecure-Requests)
    - Convert numbers/other types -> str
    """
    if not h:
        return {}
    cleaned: Dict[str, Union[str, bytes]] = {}
    for k, v in h.items():
        if v is None:
            continue
        if isinstance(v, bool):
            v = "1" if v else "0"
        elif not isinstance(v, (str, bytes)):
            v = str(v)
        cleaned[str(k)] = v  # keys must be strings too
    return cleaned


def assert_headers_are_strings(h: HeadersType, label: str = "headers") -> None:
    """Debug helper; prints any non-str/bytes values once."""
    if not h:
        return
    for k, v in h.items():
        if not isinstance(v, (str, bytes)) and v is not None:
            print(f"[BAD HEADER in {label}] {k} = {v!r} (type={type(v).__name__})")


# ───────────────────────── Browser decorator ─────────────────────

def with_browser(
    viewport: Tuple[int, int] = (1920, 1080),
    scale: float = 2.0,
    headless: bool = True,
    extra_http_headers: HeadersType = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Cross-platform Playwright context manager as a decorator.
      - Windows: event-loop policy is already set above
      - Linux (e.g., Streamlit Community Cloud): add no-sandbox/dev-shm flags
    Usage:
        @with_browser(extra_http_headers={"User-Agent": "...", "DNT": True})
        def run(ctx, ...):
            page = ctx.new_page()
            ...
    """
    def _wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
        def _inner(*args, **kwargs) -> Any:
            # Platform-aware Chromium flags (needed in many Linux/CI/cloud envs)
            launch_args = []
            if sys.platform.startswith("linux"):
                launch_args = ["--no-sandbox", "--disable-dev-shm-usage"]

            # Clean & validate headers once here so all pages inherit them
            if extra_http_headers:
                assert_headers_are_strings(extra_http_headers, "extra_http_headers (pre-clean)")
            cleaned_headers = clean_headers(extra_http_headers)

            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=headless, args=launch_args)
                ctx = browser.new_context(
                    viewport={"width": viewport[0], "height": viewport[1]},
                    device_scale_factor=scale,
                    locale="en-US",
                    extra_http_headers=cleaned_headers or None,
                )
                try:
                    return fn(ctx, *args, **kwargs)
                finally:
                    try:
                        ctx.close()
                    finally:
                        browser.close()
        return _inner
    return _wrap
