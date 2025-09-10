# provisioning/bootstrap.py
from __future__ import annotations
import subprocess

def _install_chromium():
    subprocess.run(
        ["python", "-m", "playwright", "install", "chromium"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

def _can_launch() -> bool:
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            b = pw.chromium.launch(headless=True)
            b.close()
        return True
    except Exception as e:
        s = str(e)
        # Return False when the browser binary is missing
        return not ("Executable doesn't exist" in s or "headless_shell" in s)

def ensure_playwright_ready():
    """Ensure Chromium binary is available; install if a launch test fails."""
    try:
        import playwright  # noqa: F401
    except Exception:
        raise RuntimeError("Missing dependency: add 'playwright' to requirements.txt")
    if _can_launch():
        return
    _install_chromium()
    if not _can_launch():
        raise RuntimeError("Chromium install attempted, but Playwright still cannot launch.")

# Backward-compatible alias so existing imports don’t break
ensure_playwright_installed = ensure_playwright_ready

__all__ = ["ensure_playwright_ready", "ensure_playwright_installed"]
