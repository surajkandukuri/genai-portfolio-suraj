# pages/2_Admin.py
import os, json, urllib.request, urllib.error
from typing import Dict
import streamlit as st
from supabase import create_client
from provisioning.ui import inject_styles, card
from provisioning.ui import inject_styles, card, render_sidebar

# Page + styles
st.set_page_config(page_title="Admin — Console", page_icon="🛠️", layout="centered")
inject_styles()
render_sidebar("Console")
st.title("🛠️ Admin — Console")
st.caption("Centralized agent checks and post-provision utilities.")

def sget(*keys, default=None):
    for k in keys:
        try:
            if k in st.secrets: return st.secrets[k]
        except Exception: pass
        v = os.getenv(k)
        if v: return v
    return default

def AGENTS_URL(): return sget("AGENTS_URL", default="http://localhost:7000")
def http_get_json(url:str):
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=8) as r: return json.loads(r.read().decode("utf-8"))
def http_post_json(url:str, payload:dict):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=12) as r: return json.loads(r.read().decode("utf-8"))
def is_admin(user: Dict) -> bool: return str(user.get("username","")).lower()=="centralized_uname"

SUPABASE_URL = sget("SUPABASE_URL","SUPABASE__URL")
SUPABASE_KEY = sget("SUPABASE_SERVICE_ROLE_KEY","SUPABASE_ANON_KEY","SUPABASE__SUPABASE_SERVICE_KEY")

@st.cache_resource
def get_sb():
    if not SUPABASE_URL or not SUPABASE_KEY:
        st.error("Missing Supabase config. Add SUPABASE_URL and key in .streamlit/secrets.toml"); st.stop()
    return create_client(SUPABASE_URL, SUPABASE_KEY)
sb = get_sb()

def authenticate(username:str, pwd:str):
    resp = sb.table("teams").select("*").eq("username", username).eq("pwd", pwd).execute()
    return resp.data[0] if resp.data else None

# Login
if "user" not in st.session_state: st.session_state.user = None
if st.session_state.user is None:
    with card("Login (Admin)"):
        with st.form("login_form_admin", clear_on_submit=False):
            col1, col2 = st.columns(2)
            username = col1.text_input("Username", placeholder="centralized_uname")
            pwd = col2.text_input("Password", type="password", placeholder="••••••")
            submitted = st.form_submit_button("Sign in")
        if submitted:
            user = authenticate(username.strip(), pwd)
            if user: st.session_state.user = user; st.success(f"Welcome, {user['team_name']}! 👋"); st.rerun()
            else: st.error("Invalid credentials. Please try again.")
    st.stop()

user = st.session_state.user
if not is_admin(user):
    with card("Access"):
        st.warning("Admin login required.")
        if st.button("Switch to Admin login"): st.session_state.user=None; st.rerun()
    st.stop()

# State: mutually exclusive panels
if "console" not in st.session_state:
    st.session_state.console = {"mode": None, "checks_payload": None, "psta_payload": None}
def open_checks(): st.session_state.console.update(mode="checks", psta_payload=None)
def open_psta():   st.session_state.console.update(mode="psta",   checks_payload=None)

url = AGENTS_URL()
col1, col2, col3 = st.columns(3)
with col1:
    with card("Fast API Gateway Health"):
        if st.button("Gateway Health", key="btn_gw"):
            try: st.json(http_get_json(f"{url}/health"))
            except Exception as e: st.error(f"Gateway not reachable at {url}: {e}")
with col2:
    with card("Checks Agent Health"):
        st.button("Checks Agent Health", key="btn_checks", on_click=open_checks)
        if st.session_state.console["mode"]=="checks" and st.session_state.console["checks_payload"] is None:
            try:
                st.session_state.console["checks_payload"] = http_get_json(f"{url}/agents/provisioning-checks/health")
                st.success("Checks Agent is reachable")
            except Exception as e:
                st.session_state.console["checks_payload"] = {"error": str(e)}; st.error(e)
with col3:
    with card("Post Provision Sample LLM Agent"):
        st.button("Try", key="btn_psta_try", on_click=open_psta)
        if st.session_state.console["mode"]=="psta":
            try: st.caption("Checking agent…"); st.json(http_get_json(f"{url}/agents/psta/health"))
            except Exception as e: st.error(e)

st.divider()
mode = st.session_state.console["mode"]

if mode=="checks":
    with card("Output of Agents Health Check"):
        sel_key_input = st.text_input("selection_key to verify",
                        value=str(st.session_state.get("last_selection", {}).get("selection_key","")),
                        key="sel_key_input_checks")
        if st.button("Run Checks", key="btn_run_checks"):
            try:
                out = http_post_json(f"{url}/agents/provisioning-checks/run", {"selection_key": int(sel_key_input)})
                st.success(out.get("status",""))
                if out.get("missing"): st.warning(f"Missing: {out['missing']}")
                st.caption(f"Workspace: {out.get('repo_path')}")
            except Exception as e: st.error(e)

if mode=="psta":
    with card("Post Provision Sample LLM Agent"):
        left, right = st.columns([1,1])
        with left:
            cuisine = st.selectbox("Cuisine", ["Indian","Italian","Mexican","Thai","Chinese"], key="psta_cuisine")
            if st.button("Generate Sample", key="btn_psta_gen"):
                try:
                    out = http_post_json(f"{url}/agents/psta/generate", {"cuisine": cuisine})
                    st.session_state.console["psta_payload"] = out
                    st.success(f"✨ {out.get('name')}")
                except Exception as e: st.error(e)
        with right:
            p = st.session_state.console["psta_payload"]
            if p:
                st.subheader(p.get("name",""))
                for item in (p.get("items") or []): st.markdown(f"- {item}")

st.divider()
if st.button("Sign out"):
    st.session_state.user=None
    st.session_state.console={"mode":None,"checks_payload":None,"psta_payload":None}
    st.rerun()
