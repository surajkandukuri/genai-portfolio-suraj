# pages/5_psuedocode.py
# ProvisionAgent — PsuedoCode (Email-gated, simple English overview)

from __future__ import annotations
import os, re, datetime as dt, textwrap
import streamlit as st

# Optional: Supabase logging (no-op if secrets/env not set)
try:
    from supabase import create_client, Client
except Exception:
    create_client = None
    Client = None  # type: ignore

st.set_page_config(page_title="ProvisionAgent — PsuedoCode", page_icon="📐", layout="wide")

st.title("📐 ProvisionAgent — PsuedoCode")
st.caption("Access requires a valid email. Your email is used only for access logs and follow-ups about this demo.")

# ── Helpers ─────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)

def _valid_email(e: str) -> bool:
    return bool(EMAIL_RE.match((e or "").strip()))

def _get_sb() -> Client | None:
    """Create Supabase client if secrets are available; otherwise return None."""
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

def _log_access(email: str, ok: bool, reason: str = "") -> None:
    """Best-effort access log; never blocks the page."""
    sb = _get_sb()
    if not sb:
        return
    row = {
        "email": (email or None),
        "granted": ok,
        "reason": (reason or None),
        "page": "provisionagent_psuedocode",
        "ts_utc": dt.datetime.utcnow().isoformat(),
        "user_agent": st.session_state.get("_user_agent"),
    }
    table = os.getenv("DOC_ACCESS_TABLE", "prov_doc_access_log")
    try:
        sb.table(table).insert(row).execute()
    except Exception:
        pass

# Capture UA once per session (optional)
if "_user_agent" not in st.session_state:
    try:
        st.session_state["_user_agent"] = st.runtime.scriptrunner.get_script_run_ctx().request.headers.get("user-agent")  # type: ignore
    except Exception:
        st.session_state["_user_agent"] = None

# ── Email gate ───────────────────────────────────────────────────────────────
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
            _log_access(email.strip(), ok=False, reason="invalid_email")
            st.error("Please enter a valid email address.")

if not st.session_state["prov_access_granted"]:
    st.info("Enter a valid email above to view the PsuedoCode.")
    st.stop()

st.success(f"Welcome, {st.session_state['prov_access_email']}! Access granted.")
st.divider()

# ── Simple English overview (no expanders) ───────────────────────────────────
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

            # Step 1–2: pick target env + stack
            env   = select(['DEV','QA','PROD'])
            stack = select(['DJANGO','FASTAPI','STREAMLIT','.NET','PYTHON'])

            # Step 3: choose artifacts (one of each)
            db      = choose('Database')
            secrets = choose('Secrets Manager')
            models  = choose('AI Models')
            storage = choose('Storage')

            # Step 4: (optional) save selection for audit/history
            selection_key = db_insert('team_selection_batch', {
                team_id, environment_id, target_runtime_id, by=user.username
            })
            for each (type, pick) in {db,secrets,models,storage}:
                db_insert('team_selection_detail', { selection_key, type, pick })

            # Step 5: PROVISION NOW
            team_env = slug(team_name) + '_' + slug(env)
            make_folder('apps/' + team_env)

            # write minimal app + docker bits
            write('apps/team_env/app.py', hello_streamlit_app)
            write('apps/team_env/Dockerfile', dockerfile_text)
            write('apps/team_env/docker-compose.yml', compose_for(env, team_env))
            write('apps/team_env/.env', '')               # empty by default
            write('apps/team_env/README.md', run_notes)
            write helpers: enable_agents.sh/.ps1, disable_agents.sh/.ps1

            # zip the folder and upload to Supabase Storage
            zip_path = zipdir('apps/' + team_env, name=team_env + '.zip')
            key = f"provisional_agent/{team_env}_{timestamp()}/{team_env}.zip"
            storage.upload(bucket='provisional_agent', key=key, file=zip_path, upsert=True)

            # mark batch as provisioned
            db_update('team_selection_batch', selection_key, { provision_done_ind: 'Y' })

            # show run commands to user
            return {
                repo_path: 'apps/' + team_env,
                docker_hint: 'docker compose up -d',
                storage_key: key
            }
        """
    ),
    language="text",
)

st.markdown("### Security notes")
st.markdown(
    """
- This page uses **email gating** only for demo throttling.  
  For production: add SSO or OTP, role checks, and RLS on DB tables.
- Provisioning is **idempotent**: if you re-run, we overwrite files and keep one clear audit trail.
- Secrets never show in the UI. They go to `.env` or your chosen Secrets Manager.
"""
)

# Optional quick links (if file-based navigation is enabled in your app)
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
