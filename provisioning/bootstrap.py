# provisioning/bootstrap.py
import subprocess

def ensure_playwright_installed():
    try:
        from playwright.sync_api import sync_playwright
        return
    except Exception:
        subprocess.run(
            ["python", "-m", "playwright", "install", "chromium"],
            check=True
        )
        from playwright.sync_api import sync_playwright
