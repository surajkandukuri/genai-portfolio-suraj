# provisioning/autostart_api.py
from __future__ import annotations
import os, sys, atexit, time, socket, subprocess, contextlib
import streamlit as st

def _is_listening(host: str, port: int, timeout: float = 0.25) -> bool:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(timeout)
        try:
            sock.connect((host, port))
            return True
        except Exception:
            return False

def _wait_until_up(host: str, port: int, timeout: float = 8.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _is_listening(host, port):
            return True
        time.sleep(0.15)
    return False

@st.cache_resource(show_spinner=False)
def ensure_fastapi(
    app_module: str | None = None,
    host: str | None = None,
    port: int | None = None,
) -> dict:
    """
    Start FastAPI via Uvicorn in the background if not already running.
    Returns dict: {status, url, pid}

    Env overrides:
      PA_API_AUTOSTART=1|0
      PA_API_APP=provisioning.api:app
      PA_API_HOST=127.0.0.1
      PA_API_PORT=7000
      PA_API_RELOAD=0|1
    """
    autostart = os.getenv("PA_API_AUTOSTART", "1").lower() in ("1", "true", "yes", "y")
    if not autostart:
        return {"status": "disabled", "url": None, "pid": None}

    app_module = app_module or os.getenv("PA_API_APP", "provisioning.api:app")
    host = host or os.getenv("PA_API_HOST", "127.0.0.1")
    port = int(port or os.getenv("PA_API_PORT", "7000"))
    reload_flag = os.getenv("PA_API_RELOAD", "0").lower() in ("1", "true", "yes", "y")

    url = f"http://{host}:{port}"

    if _is_listening(host, port):
        return {"status": "already-running", "url": url, "pid": None}

    cmd = [
        sys.executable, "-m", "uvicorn", app_module,
        "--host", host, "--port", str(port),
        "--workers", "1", "--log-level", "info",
    ]
    if reload_flag:
        cmd.append("--reload")

    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "uvicorn.log")
    log_file = open(log_path, "a", encoding="utf-8")

    proc = subprocess.Popen(cmd, stdout=log_file, stderr=log_file, close_fds=True)

    def _cleanup():
        with contextlib.suppress(Exception):
            proc.terminate()
    atexit.register(_cleanup)

    if not _wait_until_up(host, port, timeout=10.0):
        st.error(
            f"FastAPI failed to start on {url}. "
            f"Check {log_path} and verify PA_API_APP (module:app)."
        )
        return {"status": "failed", "url": url, "pid": proc.pid}

    return {"status": "started", "url": url, "pid": proc.pid}
