# pages/25_kpidrift_psuedocode.py
# KPI Drift Hunter — PseudoCode (Email-gated) using shared Supabase config helpers

from __future__ import annotations
import os, re, datetime as dt, textwrap, logging
import streamlit as st
import streamlit.components.v1 as components
from urllib.parse import urlparse

# ─────────────────────────── Logging ───────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("kdh.pseudocode")

# ───────────────────────────── Config helpers ─────────────────────────────
def sget(*keys, default=None):
    """Get config from Streamlit secrets (preferred) or env vars."""
    for k in keys:
        try:
            if k in st.secrets:
                return st.secrets[k]
        except Exception:
            pass
        v = os.getenv(k)
        if v:
            return v
    return default

SUPABASE_URL = sget("SUPABASE_URL", "SUPABASE__URL")
SUPABASE_KEY = sget(
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_SERVICE_KEY",
    "SUPABASE_ANON_KEY",
    "SUPABASE__SUPABASE_SERVICE_KEY",
)
DOC_ACCESS_TABLE = sget("DOC_ACCESS_TABLE", default="kdh_doc_access_log")

# ───────────────────────────── Supabase client ────────────────────────────
try:
    from supabase import create_client, Client
except Exception:
    create_client = None
    Client = None  # type: ignore

@st.cache_resource
def get_sb() -> Client:
    if not create_client:
        st.stop()
    if not SUPABASE_URL or not SUPABASE_KEY:
        st.error("Missing Supabase config. Add SUPABASE_URL and SERVICE_ROLE/ANON key in .streamlit/secrets.toml")
        st.stop()
    return create_client(SUPABASE_URL, SUPABASE_KEY)

sb = get_sb()

# ───────────────────────────── Utils ──────────────────────────────────────
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)
def _valid_email(e: str) -> bool:
    return bool(EMAIL_RE.match((e or "").strip()))

def _log_access(email: str, ok: bool, reason: str = "") -> None:
    """Insert a row into kdh_doc_access_log."""
    row = {
        "email": (email or "").strip(),
        "granted": bool(ok),
        "reason": reason or None,
        "page": "documentation",
        "ts_utc": dt.datetime.utcnow().isoformat(),
        "user_agent": st.session_state.get("_user_agent"),
    }
    try:
        sb.postgrest.schema("public").from_(DOC_ACCESS_TABLE).insert(row).execute()
    except Exception as e:
        LOG.exception("Insert FAILED")
        st.error(f"Log insert failed: {e}")

def to_embed_url(public_url: str) -> str:
    path_last = urlparse(public_url).path.rstrip("/").split("/")[-1]
    embed_id = path_last.split("-")[-1]
    return f"https://dbdiagram.io/embed/{embed_id}"

# ─────────────────────────── Page setup ───────────────────────────────────
DBDIAGRAM_URL = "https://dbdiagram.io/d/KDH-68c2fa4e841b2935a614ca3f"
EMBED_URL = to_embed_url(DBDIAGRAM_URL)

st.set_page_config(page_title="KPI Drift Hunter — PseudoCode",
                   page_icon="📖", layout="wide")

st.title("📖 KPI Drift Hunter — PseudoCode")
st.caption("Access requires a valid email. Your email is used only for access logs and follow-ups about this demo.")

# Capture user-agent once per session
if "_user_agent" not in st.session_state:
    try:
        st.session_state["_user_agent"] = st.runtime.scriptrunner.get_script_run_ctx().request.headers.get("user-agent")  # type: ignore
    except Exception:
        st.session_state["_user_agent"] = None

if "docs_access_granted" not in st.session_state:
    st.session_state["docs_access_granted"] = False
    st.session_state["docs_access_email"] = ""

# ───────────────────────────── Gate UI ────────────────────────────────────
with st.container(border=True):
    st.subheader("Request Access")
    left, right = st.columns([3, 1])
    with left:
        email = st.text_input(
            "Work or personal email",
            value=st.session_state["docs_access_email"],
            placeholder="you@company.com",
        )
    with right:
        request = st.button("Unlock", type="primary", use_container_width=True)

    if request:
        if _valid_email(email):
            st.session_state["docs_access_granted"] = True
            st.session_state["docs_access_email"] = email.strip()
            _log_access(email.strip(), ok=True)
            st.success("Access granted for this session.")
        else:
            _log_access(email.strip(), ok=False, reason="invalid_email")
            st.error("Please enter a valid email address.")

if not st.session_state["docs_access_granted"]:
    st.info("Enter a valid email above to view the PseudoCode.")
    st.stop()

# ───────────────────────────── Content ────────────────────────────────────
st.success(f"Welcome, {st.session_state['docs_access_email']}! Access granted.")

st.markdown("### 📐 System Overview")
st.markdown("""
**KPI Drift Hunter** solves cross-tool KPI inconsistencies by automating:
1) **Capture** of full dashboards and per-chart crops  
2) **Understand** via OCR/structure to produce normalized KPI rows  
3) **Compare** between tools to flag drift (naming, units, shape, and level)
""")

with st.expander("Agent A — Capture", expanded=True):
    st.code(textwrap.dedent("""
    function capture_url(url, session_id):
        # ...
    """), language="text")

with st.expander("Agent B — Understand", expanded=True):
    st.code(textwrap.dedent("""
    function parse_widgets_to_rows(widgets):
        # ...
    """), language="text")

with st.expander("Agent C — Compare", expanded=True):
    st.code(textwrap.dedent("""
    function compare_series(a_df, b_df):
        # ...
    """), language="text")

with st.expander("Data Model (v1)", expanded=False):
    st.link_button("Open interactive diagram ↗", DBDIAGRAM_URL, use_container_width=True)
    components.iframe(EMBED_URL, height=760, scrolling=True)

with st.expander("Security & Access Notes", expanded=False):
    st.markdown("""
- This page gates content by email only to deter casual scraping.  
- For production, add domain allow-list, OTP/magic link, or signed tokens.
    """)

st.divider()
st.caption("© KPI Drift Hunter — Documentation (access logged).")
