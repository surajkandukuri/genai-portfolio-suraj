import sys, os, subprocess, streamlit as st
from provisioning.bootstrap import ensure_playwright_ready
# at top of pages/00_playwright_diag.py
import sys, pathlib
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from provisioning.bootstrap import ensure_playwright_ready

st.title("Playwright Diag")

st.write("Python:", sys.version)
st.write("Exec:", sys.executable)
st.write("PLAYWRIGHT_BROWSERS_PATH:", os.environ.get("PLAYWRIGHT_BROWSERS_PATH"))

# Try CLI
code = subprocess.run([sys.executable, "-m", "playwright", "--version"], capture_output=True, text=True)
st.write("CLI:", code.stdout or code.stderr)

# Install/launch once
try:
    ensure_playwright_ready()
    st.success("ensure_playwright_ready() OK — Chromium can launch.")
except Exception as e:
    st.error(f"Bootstrap failed: {e}")
    raise
