# streamlit_app.py
import os
from typing import List, Dict
import streamlit as st
from supabase import create_client, Client

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="AI Environment Provisioning Portal", page_icon="🧭", layout="centered")

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
SUPABASE_KEY = sget("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ANON_KEY", "SUPABASE__SUPABASE_SERVICE_KEY")

@st.cache_resource
def get_sb() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        st.error("Missing Supabase config. Add SUPABASE_URL and SERVICE_ROLE/ANON key in .streamlit/secrets.toml")
        st.stop()
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        st.error(f"Could not connect to Supabase: {e}")
        st.stop()

sb = get_sb()

# Health check
try:
    sb.table("environment").select("*").limit(1).execute()
    st.caption("✅ Connected to Supabase.")
except Exception as e:
    st.error(f"DB check failed. Did you run the DDL? Error: {e}")
    st.stop()

# ── Data helpers ──────────────────────────────────────────────────────────────
def authenticate(username: str, pwd: str):
    resp = sb.table("teams").select("*").eq("username", username).eq("pwd", pwd).execute()
    if resp.data:
        return resp.data[0]
    return None

@st.cache_data(ttl=60)
def load_environments() -> List[Dict]:
    return sb.table("environment").select("*").order("environment_id").execute().data

@st.cache_data(ttl=60)
def load_artifact_types() -> List[Dict]:
    return sb.table("artifact_type").select("*").order("artifact_type_id").execute().data

@st.cache_data(ttl=60)
def load_artifacts_by_type(artifact_type_id: int) -> List[Dict]:
    return sb.table("artifacts").select("*").eq("artifact_type_id", artifact_type_id).order("artifact_name").execute().data

@st.cache_data(ttl=60)
def load_target_runtimes() -> List[Dict]:
    # Make sure you've created the target_runtime table per the DDL we discussed
    return sb.table("target_runtime").select("*").order("target_runtime_id").execute().data

def create_selection_batch(team_id: int, environment_id: int, username: str, target_runtime_id: int) -> int:
    """Insert header row (with target runtime) and return selection_key."""
    resp = (
        sb.table("team_selection_batch")
        .insert(
            {
                "team_id": team_id,
                "environment_id": environment_id,
                "insrt_user_name": username,
                "target_runtime_id": target_runtime_id,
            },
            returning="representation"  # ensures inserted row is returned
        )
        .execute()
    )
    if not resp.data or not isinstance(resp.data, list):
        raise RuntimeError(f"Insert did not return a row: {resp}")
    return int(resp.data[0]["selection_key"])

def save_detail(selection_key: int, artifact_type_id: int, artifact_id: int):
    sb.table("team_selection_detail").insert({
        "selection_key": selection_key,
        "artifact_type_id": artifact_type_id,
        "artifact_id": artifact_id
    }).execute()

# ── UI ────────────────────────────────────────────────────────────────────────
st.title("🧭 AI Environment Provisioning Portal")
st.caption("Select approved components and provision standardized environments in minutes.")

# Session user
if "user" not in st.session_state:
    st.session_state.user = None

# Login
if st.session_state.user is None:
    st.subheader("Login")
    with st.form("login_form", clear_on_submit=False):
        col1, col2 = st.columns(2)
        username = col1.text_input("Username", placeholder="team_1")
        pwd = col2.text_input("Password", type="password", placeholder="••••••")
        submitted = st.form_submit_button("Sign in")
    if submitted:
        user = authenticate(username.strip(), pwd)
        if user:
            st.session_state.user = user
            st.success(f"Welcome, {user['team_name']}! 👋")
            st.rerun()
        else:
            st.error("Invalid credentials. Please try again.")
    st.stop()

# After login
user = st.session_state.user
st.write(
    f"**Team:** {user['team_name']} • **POC:** {user.get('team_pointofcontact') or '—'} • "
    f"**DL:** {user.get('team_distributionlist') or '—'} • **User:** {user['username']}"
)
st.divider()

# Environment dropdown
envs = load_environments()
env_options = {e["environment_name"].upper(): e["environment_id"] for e in envs}
env_label = st.selectbox("Select Environment", list(env_options.keys()))
environment_id = env_options[env_label]

# Target runtime dropdown (the bit you asked for)
try:
    runtimes = load_target_runtimes()
    if not runtimes:
        st.warning("No target runtimes found. Did you insert rows into target_runtime?")
        st.stop()
    rt_options = {r["target_runtime"].upper(): r["target_runtime_id"] for r in runtimes}
    rt_label = st.selectbox("Target Environment", list(rt_options.keys()))
    target_runtime_id = rt_options[rt_label]
except Exception as e:
    st.error(f"Failed to load target runtimes: {e}")
    st.stop()

st.subheader("Select Approved Artifacts (one from each)")

artifact_types = load_artifact_types()

# Build one dropdown per artifact type
picks = {}
for at in artifact_types:
    at_id = at["artifact_type_id"]
    at_name = at["artifact_type"]
    data = load_artifacts_by_type(at_id)
    options = {row["artifact_name"]: row["artifact_id"] for row in data}
    if not options:
        st.warning(f"No artifacts configured for **{at_name}** yet.")
        continue
    selection = st.selectbox(
        f"{at_name.title()}",
        ["-- choose --"] + list(options.keys()),
        key=f"sb_{at_id}"
    )
    picks[at_id] = options.get(selection) if selection != "-- choose --" else None

# Must pick all types
all_chosen = all(picks.get(at["artifact_type_id"]) for at in artifact_types)

col_l, col_r = st.columns([1, 1])

# Save button → header (with runtime) + details with same selection_key
if col_l.button("📦 Save Selection", type="primary", disabled=not all_chosen):
    try:
        selection_key = create_selection_batch(user["team_id"], environment_id, user["username"], target_runtime_id)
        for at in artifact_types:
            at_id = at["artifact_type_id"]
            art_id = picks.get(at_id)
            save_detail(selection_key, at_id, art_id)
        st.success(f"Saved under selection_key **{selection_key}** for **{env_label} / {rt_label}** ✅")
    except Exception as e:
        st.error(f"Save failed: {e}")

# History (optional; relies on v_team_selection_flat view if you created it)
with col_r.expander("📜 View My Past Selections"):
    try:
        resp = (
            sb.table("v_team_selection_flat")
            .select("*")
            .eq("team_id", user["team_id"])
            .order("selection_key", desc=True)
            .limit(100)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            st.info("No selections yet.")
        else:
            st.write([
                {
                    "selection_key": r["selection_key"],
                    "when": r["insrt_dttm"],
                    "environment": r["environment_name"],
                    "type": r["artifact_type_name"],
                    "artifact": r["artifact_name"],
                    "by": r["insrt_user_name"],
                } for r in rows
            ])
    except Exception as e:
        st.warning(f"Could not load history view: {e}")

st.divider()
if st.button("Sign out"):
    st.session_state.user = None
    st.rerun()
