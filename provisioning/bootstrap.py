from __future__ import annotations
import os, sys, subprocess

def _run(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.returncode, (r.stdout or "") + ("\n" + r.stderr if r.stderr else "")

def _can_launch() -> bool:
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            b = p.chromium.launch(headless=True)
            b.close()
        return True
    except Exception:
        return False

def ensure_playwright_ready():
    # Use a cache path that works on Cloud
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", os.path.expanduser("~/.cache/ms-playwright"))

    try:
        import playwright  # noqa: F401
    except Exception:
        raise RuntimeError("Missing dependency: add 'playwright' to requirements.txt")

    if _can_launch():
        return

    # Install using the SAME interpreter Streamlit runs
    code, out = _run([sys.executable, "-m", "playwright", "install", "chromium"])
    if code != 0:
        try:
            import streamlit as st
            st.error("Playwright browser install failed.")
            st.code(out)
        except Exception:
            pass
        raise RuntimeError("playwright install chromium failed")

    if not _can_launch():
        raise RuntimeError("Chromium installed but still cannot launch.")

# Back-compat alias if some files still import the old name
ensure_playwright_installed = ensure_playwright_ready
__all__ = ["ensure_playwright_ready", "ensure_playwright_installed"]
