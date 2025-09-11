# pages/24_kpidrift_documentation.py
# KPI Drift Hunter — Documentation (Email-gated)

from __future__ import annotations
import os, re, datetime as dt, textwrap
import streamlit as st
import streamlit.components.v1 as components
from urllib.parse import urlparse
DBDIAGRAM_URL = "https://dbdiagram.io/d/KDH-68c2fa4e841b2935a614ca3f"

# Optional: Supabase logging (will no-op if secrets aren’t set)
try:
    from supabase import create_client, Client
except Exception:
    create_client = None
    Client = None  # type: ignore

st.set_page_config(page_title="KPI Drift Hunter — Documentation", page_icon="📖", layout="wide")

st.title("📖 KPI Drift Hunter — Documentation")
st.caption("Access requires a valid email. Your email is used only for access logs and follow-ups about this demo.")

# ── Helpers ─────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)

def _valid_email(e: str) -> bool:
    return bool(EMAIL_RE.match((e or "").strip()))

def _get_sb() -> Client | None:
    if not create_client:
        return None
    url = os.getenv("SUPABASE_URL") or st.secrets.get("SUPABASE_URL", "")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or st.secrets.get("SUPABASE_SERVICE_ROLE_KEY", "")
        or st.secrets.get("SUPABASE_ANON_KEY", "")
    )
    if not url or not key:
        return None
    try:
        return create_client(url, key)
    except Exception:
        return None
def to_embed_url(public_url: str) -> str:
    """
    Convert a dbdiagram public link to its 'embed' form so only the diagram shows.
    Works for both '/d/<id>' and '/d/<slug>-<id>'.
    """
    path_last = urlparse(public_url).path.rstrip("/").split("/")[-1]  # e.g. 'KDH-68c2fa4e841b2935a614ca3f'
    embed_id = path_last.split("-")[-1]                               # -> '68c2fa4e841b2935a614ca3f'
    return f"https://dbdiagram.io/embed/{embed_id}"

EMBED_URL = to_embed_url(DBDIAGRAM_URL)
def _log_access(email: str, ok: bool, reason: str = "") -> None:
    """Optional: write to a small audit table."""
    sb = _get_sb()
    if not sb:
        return
    table = os.getenv("DOC_ACCESS_TABLE", "kdh_doc_access_log")
    row = {
        "email": email,
        "granted": ok,
        "reason": reason or None,
        "page": "documentation",
        "ts_utc": dt.datetime.utcnow().isoformat(),
        "user_agent": st.session_state.get("_user_agent"),
    }
    try:
        sb.table(table).insert(row).execute()
    except Exception:
        pass  # don't block page on logging errors

# (Optional) capture UA once per session
if "_user_agent" not in st.session_state:
    try:
        st.session_state["_user_agent"] = st.runtime.scriptrunner.get_script_run_ctx().request.headers.get("user-agent")  # type: ignore
    except Exception:
        st.session_state["_user_agent"] = None

# ── Gate UI ─────────────────────────────────────────────────────────────────
if "docs_access_granted" not in st.session_state:
    st.session_state["docs_access_granted"] = False
    st.session_state["docs_access_email"] = ""

with st.container(border=True):
    st.subheader("Request Access")
    left, right = st.columns([3,1])
    with left:
        email = st.text_input("Work or personal email", value=st.session_state["docs_access_email"], placeholder="you@company.com")
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
    st.info("Enter a valid email above to view documentation.")
    st.stop()

# ── Content (visible after gate) ────────────────────────────────────────────
st.success(f"Welcome, {st.session_state['docs_access_email']}! Access granted.")

st.markdown("### 📐 System Overview")
st.markdown("""
**KPI Drift Hunter** solves cross-tool KPI inconsistencies by automating:
1) **Capture** of full dashboards and per-chart crops  
2) **Understand** via OCR/structure to produce normalized KPI rows  
3) **Compare** between tools to flag drift (naming, units, shape, and level)
""")

with st.expander("Agent A — Capture (Login ➜ Screenshot ➜ Store)", expanded=True):
    st.code(textwrap.dedent("""
    function capture_url(url, session_id):
        ensure_playwright_ready()
        provider = classify(url)   # 'powerbi' | 'tableau' | 'unknown'

        if provider == 'powerbi':
            result = capture_powerbi(url, outdir='screenshots/')
        else:
            result = capture_tableau(url, outdir='screenshots/')   # public / cloud via orchestrator

        # Upload
        full_png = read(result.artifacts.full)
        full_key = f"{bucket_prefix(session_id, url)}/full.png"
        storage.upload(full_key, full_png)

        # DB (parent)
        screengrab = db.upsert_kdh_screengrab(
            url=url, platform=provider, storage_path_full=full_key, hash=sha256(full_png)
        )

        # Widgets (if single report crop exists)
        if result.artifacts.report:
            crop_png = read(result.artifacts.report)
            crop_key = f"{bucket_prefix(session_id, url)}/widgets/report_crop.png"
            storage.upload(crop_key, crop_png)
            db.insert_kdh_widgets(parent=screengrab.id, widgets=[{path: crop_key, bbox: [0,0,w,h]}])

        return { screengrab, widgets }
    """), language="text")

with st.expander("Agent B — Understand (OCR ➜ Structure ➜ Model)", expanded=True):
    st.code(textwrap.dedent("""
    function parse_widgets_to_rows(widgets):
        rows = []
        for each widget in widgets:
            # Prefer DOM text when available; else OCR
            title, labels, units, agg = extract_metadata(widget)
            series = extract_series(widget)          # [(time_key, value), ...]
            for (time_key, value) in series:
                rows.append({
                    kpi_name: normalize(title),
                    series_name: series.name,
                    time_key, value, unit: units, agg, chart_type: guess(widget),
                    filters: widget.filters, confidence: widget.conf
                })
        return rows
    """), language="text")

with st.expander("Agent C — Compare (Row vs Row)", expanded=True):
    st.code(textwrap.dedent("""
    function compare_series(a_df, b_df):
        df = align_on_time_key(a_df, b_df)
        corr = correlation(df.value_a, df.value_b)
        mape = mean_abs_pct_error(df.value_a, df.value_b)
        issues = []
        if a_df.unit != b_df.unit: issues.append("unit_mismatch")
        if a_df.agg  != b_df.agg:  issues.append("aggregation_mismatch")

        verdict = "consistent"      if corr > 0.95 and mape < 0.02 and not issues else \
                  "likely_mismatch" if corr > 0.80 else "conflict"

        return verdict, { corr, mape, issues }
    """), language="text")

with st.expander("Data Model (v1)", expanded=False):
    st.link_button("Open interactive diagram ↗", DBDIAGRAM_URL, use_container_width=True)
    components.iframe(EMBED_URL, height=760, scrolling=True)
    st.caption("If the embed looks blank, open the diagram in a new tab.")
    
with st.expander("Security & Access Notes", expanded=False):
    st.markdown("""
- This page gates content by email only to deter casual scraping.
- For production, add one or more of:
  - Domain allow-list (e.g., company.com only)
  - OTP / magic link verification
  - reCAPTCHA
  - Signed view tokens (JWT) tied to RLS policies in Supabase
    """)

st.divider()
st.caption("© KPI Drift Hunter — Documentation (access logged).")
