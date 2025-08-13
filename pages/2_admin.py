# pages/2_Admin.py
import os
import json
import urllib.request, urllib.error
from typing import Dict

import streamlit as st
from supabase import create_client

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Admin", page_icon="🛠️", layout="centered")

# ── Shared config/helpers ─────────────────────────────────────────────────────
def sget(*keys, default=None):
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

def AGENTS_URL():
    return sget("AGENTS_URL", default="http://localhost:7000")

def http_get_json(url: str):
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=8) as r:
        return json.loads(r.read().decode("utf-8"))

def http_post_json(url: str, payload: dict):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=12) as r:
        return json.loads(r.read().decode("utf-8"))

def is_admin(user: Dict) -> bool:
    return str(user.get("username", "")).lower() == "centralized_uname"

SUPABASE_URL = sget("SUPABASE_URL", "SUPABASE__URL")
SUPABASE_KEY = sget("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ANON_KEY", "SUPABASE__SUPABASE_SERVICE_KEY")

@st.cache_resource
def get_sb():
    if not SUPABASE_URL or not SUPABASE_KEY:
        st.error("Missing Supabase config. Add SUPABASE_URL and SERVICE_ROLE/ANON key in .streamlit/secrets.toml")
        st.stop()
    return create_client(SUPABASE_URL, SUPABASE_KEY)

sb = get_sb()

def authenticate(username: str, pwd: str):
    resp = sb.table("teams").select("*").eq("username", username).eq("pwd", pwd).execute()
    if resp.data:
        return resp.data[0]
    return None

# ── UI ────────────────────────────────────────────────────────────────────────
st.title("🛠️ Admin — Centralized Agents")

# Session user
if "user" not in st.session_state:
    st.session_state.user = None

# Login
if st.session_state.user is None:
    st.subheader("Login (Admin)")
    with st.form("login_form_admin", clear_on_submit=False):
        col1, col2 = st.columns(2)
        username = col1.text_input("Username", placeholder="centralized_uname")
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

user = st.session_state.user

# Gate access
if not is_admin(user):
    st.error("You are not authorized to view this page.")
    st.stop()

# Admin content
url = AGENTS_URL()

cols = st.columns(3)
with cols[0]:
    if st.button("Gateway Health"):
        try:
            st.json(http_get_json(f"{url}/health"))
        except Exception as e:
            st.error(f"Gateway not reachable at {url}: {e}")
with cols[1]:
    if st.button("Checks Agent Health"):
        try:
            st.json(http_get_json(f"{url}/agents/provisioning-checks/health"))
        except Exception as e:
            st.error(e)
with cols[2]:
    if st.button("PSTA Agent Health"):
        try:
            st.json(http_get_json(f"{url}/agents/psta/health"))
        except Exception as e:
            st.error(e)

st.subheader("Run ProvisioningChecksAgent")
sel_key_input = st.text_input("selection_key to verify", value=str(st.session_state.get("last_selection", {}).get("selection_key", "")))
if st.button("Run Checks"):
    try:
        out = http_post_json(f"{url}/agents/provisioning-checks/run", {"selection_key": int(sel_key_input)})
        st.success(out.get("status", ""))
        if out.get("missing"):
            st.warning(f"Missing: {out['missing']}")
        st.caption(f"Workspace: {out.get('repo_path')}")
    except Exception as e:
        st.error(e)

st.subheader("Run SampleLLMWebCall (PSTA)")
cuisine = st.selectbox("Cuisine", ["Indian", "Italian", "Mexican", "Thai", "Chinese"])
if st.button("Generate Sample"):
    try:
        out = http_post_json(f"{url}/agents/psta/generate", {"cuisine": cuisine})
        st.success(f"✨ {out.get('name')}")
        st.write(out.get("items", []))
    except Exception as e:
        st.error(e)

st.divider()
if st.button("Sign out"):
    st.session_state.user = None
    st.rerun()
