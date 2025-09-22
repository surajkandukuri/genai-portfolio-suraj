# pages/5_psuedocode.py
# ProvisionAgent — PsuedoCode (Email-gated; logs to public.kdh_doc_access_log)

from __future__ import annotations
import os, re, datetime as dt, textwrap, logging
import streamlit as st

st.set_page_config(page_title="ProvisionAgent — PsuedoCode", page_icon="📐", layout="wide")
st.caption(f"Loaded from: {__file__}")

# ─────────────────────────── Logging ───────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("prov.pseudocode")

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
DOC_ACCESS_TABLE = "kdh_doc_access_log"  # <-- same table as KPI Drift page

# ───────────────────────────── Supabase client ────────────────────────────
try:
    from supabase import create_client, Client
except Exception:
    create_client = None
    Client = None  # type: ignore

@st.cache_resource
def get_sb() -> Client | None:
    if not create_client:
        return None
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        return None

sb = get_sb()

# ───────────────────────────── Utils ──────────────────────────────────────
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)
def _valid_email(e: str) -> bool:
    return bool(EMAIL_RE.match((e or "").strip()))

def _log_access(email: str, ok: bool, reason: str = "") -> None:
    """Write to public.kdh_doc_access_log (best-effort, non-blocking)."""
    if not sb:
        return
    row = {
        "email": (email or "").strip() or None,
        "granted": bool(ok),
        "reason": reason or None,
        "page": "provisionagent_psuedocode",        # page tag
        "ts_utc": dt.datetime.utcnow().isoformat(), # DB also has default now()
        "user_agent": st.session_state.get("_user_agent"),
    }
    try:
        # Force the public schema and exact table
        sb.postgrest.schema("public").from_(DOC_ACCESS_TABLE).insert(row).execute()
    except Exception as e:
        LOG.warning("Access log insert failed: %s", e)

# Capture UA once per session (optional)
if "_user_agent" not in st.session_state:
    try:
        st.session_state["_user_agent"] = st.runtime.scriptrunner.get_script_run_ctx().request.headers.get("user-agent")  # type: ignore
    except Exception:
        st.session_state["_user_agent"] = None

# ───────────────────────────── UI ─────────────────────────────────────────
st.title("📐 ProvisionAgent — PsuedoCode")
st.caption("Access requires a valid email. Your email is used only for access logs and follow-ups about this demo.")

# Email gate
if "prov_access_granted" not in st.session_state:
    st.session_state["prov_access_granted"] = False
    st.session_state["prov_access_email"] = ""

with st.container(border=True):
    st.subheader("Request Access")
    c1, c2 = st.columns([3, 1])
    with c1:
        email = st.text_input(
            "Work or personal email",
            value=st.session_state["prov_access_email"],
            placeholder="you@company.com",
        )
    with c2:
        request = st.button("Unlock", type="primary", use_container_width=True)

    if request:
        if _valid_email(email):
            st.session_state["prov_access_granted"] = True
            st.session_state["prov_access_email"] = email.strip()
            _log_access(email.strip(), ok=True)
            st.success("Access granted for this session.")
        else:
            _log_access((email or "").strip(), ok=False, reason="invalid_email")
            st.error("Please enter a valid email address.")

if not st.session_state["prov_access_granted"]:
    st.info("Enter a valid email above to view the PsuedoCode.")
    st.stop()

st.success(f"Welcome, {st.session_state['prov_access_email']}! Access granted.")
st.divider()

# ───────────────────────────── Content ────────────────────────────────────
st.markdown("### What the centralized team does")
st.markdown(
    """
1. **Log in.** You’re a centralized team admin.
2. **Pick a Target Environment.** DEV / QA / PROD.
3. **Pick a Target Stack.** e.g., DJANGO / FASTAPI / STREAMLIT / .NET / PYTHON.
4. **Choose one approved artifact from each list.** Database, Secrets Manager, AI Models, Storage.
5. **Save Selection (optional).** Keeps a history/audit trail.
6. **Provision Now.** The app scaffolds a workspace, writes docker files, and records the audit rows.
"""
)

st.markdown("### What we generate for you")
st.markdown(
    """
- A new folder like `apps/<team>_<env>/` with:
  - `app.py` (hello-app to prove the container is alive)  
  - `Dockerfile`  
  - `docker-compose.yml` (mapped to a fixed local port per env)  
  - `.env` (empty by default; can be toggled to point at a shared LLM gateway)  
  - `README.md` with run instructions  
  - helper scripts: `enable_agents.sh/.ps1` and `disable_agents.sh/.ps1`

- **Zip of that folder** saved locally and uploaded to Supabase Storage
  under `provisional_agent/<team>_<env>_<timestamp>/<team>_<env>.zip`.

- Audit updates in your DB:
  - `team_selection_batch.provision_done_ind = 'Y'`
  - Optional access log row for this page (if enabled).
"""
)

st.markdown("### How to run the generated app")
st.code(
    textwrap.dedent(
        """
        # On your machine:
        cd apps/<team_env>
        docker compose up -d
        # Open the URL shown in the UI (e.g., http://localhost:8511)
        """
    ),
    language="bash",
)

st.markdown("### PsuedoCode (high-level)")
st.code(
    textwrap.dedent(
        """
        function provision_flow(user):
            assert user.is_centralized_team
            env   = select(['DEV','QA','PROD'])
            stack = select(['DJANGO','FASTAPI','STREAMLIT','.NET','PYTHON'])
            db      = choose('Database'); secrets = choose('Secrets Manager')
            models  = choose('AI Models'); storage = choose('Storage')
            selection_key = db_insert('team_selection_batch', {...})
            for each (type, pick) in {db,secrets,models,storage}:
                db_insert('team_selection_detail', { selection_key, type, pick })
            team_env = slug(team_name) + '_' + slug(env)
            make_folder('apps/' + team_env)
            write('apps/team_env/app.py', hello_streamlit_app)
            write('apps/team_env/Dockerfile', dockerfile_text)
            write('apps/team_env/docker-compose.yml', compose_for(env, team_env))
            write('apps/team_env/.env', '')
            write('apps/team_env/README.md', run_notes)
            zip_path = zipdir('apps/' + team_env, name=team_env + '.zip')
            key = f"provisional_agent/{team_env}_{timestamp()}/{team_env}.zip"
            storage.upload(bucket='provisional_agent', key=key, file=zip_path, upsert=True)
            db_update('team_selection_batch', selection_key, { provision_done_ind: 'Y' })
            return { repo_path: 'apps/' + team_env, docker_hint: 'docker compose up -d', storage_key: key }
        """
    ),
    language="text",
)

st.markdown("### Security notes")
st.markdown(
    """
- This page uses **email gating** only for demo throttling.  
  For production: add SSO or OTP, role checks, and RLS on DB tables.
- Provisioning is **idempotent** and auditable.
- Secrets never show in the UI.
"""
)

try:
    st.divider()
    cols = st.columns(3)
    with cols[0]:
        st.page_link("pages/1_provision.py", label="Open Provision ▶️", icon="🛠️")
    with cols[1]:
        st.page_link("pages/4_Artifacts.py", label="Open Artifacts ▶️", icon="📦")
    with cols[2]:
        st.page_link("pages/9_Logout.py", label="Logout ▶️", icon="🚪")
except Exception:
    pass

st.divider()
st.caption("© ProvisionAgent — PsuedoCode (centralized team flow)")
