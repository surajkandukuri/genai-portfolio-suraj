# pages/3_Reports.py
import os
import streamlit as st
import pandas as pd
from supabase import create_client, Client
from provisioning.ui import inject_styles, card, render_sidebar
from provisioning.ui import inject_styles, card
from provisioning.theme import page_header
from provisioning.ui import card

# ── Page setup ────────────────────────────────────────────────────────────────
#st.set_page_config(page_title="Admin — Reports", page_icon="📊", layout="centered")
page_header("Deployed artifacts & provisioning history.")


# ── Config / Supabase ────────────────────────────────────────────────────────
def sget(*keys, default=None):
    for k in keys:
        try:
            if k in st.secrets: return st.secrets[k]
        except Exception:
            pass
        v = os.getenv(k)
        if v: return v
    return default

SUPABASE_URL = sget("SUPABASE_URL", "SUPABASE__URL")
SUPABASE_KEY = sget("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ANON_KEY", "SUPABASE__SUPABASE_SERVICE_KEY")

@st.cache_resource
def get_sb() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        st.error("Missing Supabase config. Add SUPABASE_URL and SERVICE_ROLE/ANON key in .streamlit/secrets.toml")
        st.stop()
    return create_client(SUPABASE_URL, SUPABASE_KEY)

sb = get_sb()

# ── Auth (admin only) ────────────────────────────────────────────────────────
def is_admin(user) -> bool:
    return str(user.get("username", "")).lower() == "centralized_uname"

def authenticate(username: str, pwd: str):
    resp = sb.table("teams").select("*").eq("username", username).eq("pwd", pwd).execute()
    return resp.data[0] if resp.data else None

if "user" not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    with card("Login (Admin)"):
        with st.form("login_form_admin_reports", clear_on_submit=False):
            c1, c2 = st.columns(2)
            username = c1.text_input("Username", placeholder="centralized_uname")
            pwd = c2.text_input("Password", type="password", placeholder="••••••")
            submitted = st.form_submit_button("Sign in")
        if submitted:
            user = authenticate(username.strip(), pwd)
            if user: st.session_state.user = user; st.success(f"Welcome, {user['team_name']}!"); st.rerun()
            else: st.error("Invalid credentials.")
    st.stop()

user = st.session_state.user
if not is_admin(user):
    with card("Access"):
        st.warning("Admin login required.")
        if st.button("Switch to Admin login"): st.session_state.user=None; st.rerun()
    st.stop()

# ── Loaders ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=120)
def load_teams():
    res = sb.table("teams").select("team_id, team_name").order("team_name").execute()
    return res.data or []

@st.cache_data(ttl=120)
def load_envs():
    res = sb.table("environment").select("environment_name").order("environment_id").execute()
    return [r["environment_name"] for r in (res.data or [])]

@st.cache_data(ttl=120)
def load_runtimes():
    res = sb.table("target_runtime").select("target_runtime").order("target_runtime_id").execute()
    return [r["target_runtime"] for r in (res.data or [])]

@st.cache_data(ttl=60)
def load_history(team_id=None, env=None, runtime=None, limit=500):
    # Prefer view v_team_selection_flat
    try:
        q = (sb.table("v_team_selection_flat")
               .select("*")
               .order("selection_key", desc=True)
               .limit(limit))
        if team_id:
            q = q.eq("team_id", team_id)
        if env and env != "ALL":
            q = q.eq("environment_name", env)
        if runtime and runtime != "ALL":
            q = q.eq("target_runtime", runtime)
        res = q.execute()
        return res.data or []
    except Exception:
        # Fallback to batches table only (reduced info)
        q = (sb.table("team_selection_batch")
               .select("*")
               .order("selection_key", desc=True)
               .limit(limit))
        if team_id:
            q = q.eq("team_id", team_id)
        res = q.execute()
        return res.data or []

# ── Filters ───────────────────────────────────────────────────────────────────
teams = load_teams()
team_map = {"ALL": None} | {t["team_name"]: t["team_id"] for t in teams}
envs = ["ALL"] + load_envs()
rts  = ["ALL"] + load_runtimes()

with card("Filters"):
    c1, c2, c3, c4 = st.columns([1.4, 1, 1, 0.8])
    team_label = c1.selectbox("Team", list(team_map.keys()))
    env_label  = c2.selectbox("Environment", envs)
    rt_label   = c3.selectbox("Runtime", rts)
    limit      = c4.number_input("Limit", min_value=50, max_value=2000, value=500, step=50)

rows = load_history(team_map[team_label], env_label, rt_label, limit)

# ── Tables ────────────────────────────────────────────────────────────────────
if not rows:
    with card("Results"):
        st.info("No data yet.")
else:
    with card("Provisioning History"):
        # choose readable columns if available
        pref = ["selection_key","insrt_dttm","team_name","environment_name","target_runtime",
                "artifact_type_name","artifact_name","insrt_user_name"]
        cols = [c for c in pref if c in rows[0].keys()] or list(rows[0].keys())
        df = pd.DataFrame(rows)[cols]
        st.dataframe(df, use_container_width=True, hide_index=True)

        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", csv, file_name="provision_history.csv", mime="text/csv")
