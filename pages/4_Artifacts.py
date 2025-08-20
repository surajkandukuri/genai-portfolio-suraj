# pages/4_Artifacts.py
import os
import streamlit as st
import pandas as pd
from supabase import create_client, Client
from provisioning.ui import inject_styles, card, render_sidebar
from provisioning.ui import inject_styles, card

from provisioning.theme import page_header
from provisioning.ui import card

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Admin — Artifacts", page_icon="🧩", layout="centered")
page_header("Browse approved artifacts by type; quick search & details.")



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
        with st.form("login_form_admin_artifacts", clear_on_submit=False):
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
def load_types():
    res = sb.table("artifact_type").select("artifact_type_id, artifact_type").order("artifact_type_id").execute()
    return res.data or []

@st.cache_data(ttl=120)
def load_by_type(artifact_type_id: int):
    res = (sb.table("artifacts")
             .select("*")
             .eq("artifact_type_id", artifact_type_id)
             .order("artifact_name")
             .execute())
    return res.data or []

types = load_types()
if not types:
    with card("Artifacts"):
        st.warning("No artifact types found. Insert rows into artifact_type.")
    st.stop()

type_labels = {t["artifact_type"]: t["artifact_type_id"] for t in types}

with card("Filters"):
    c1, c2 = st.columns([1.2, 1])
    at_label = c1.selectbox("Artifact Type", list(type_labels.keys()))
    q = c2.text_input("Search", placeholder="Filter by name, version, owner, description...")

rows = load_by_type(type_labels[at_label])

# simple in-memory filter
if q:
    ql = q.lower()
    rows = [r for r in rows if any(ql in str(v).lower() for v in r.values())]

if not rows:
    with card("Results"):
        st.info("No artifacts match.")
else:
    with card("Artifacts"):
        # Prefer readable columns if they exist
        prefer = ["artifact_name","version","owner","artifact_desc","updated_at"]
        show_cols = [c for c in prefer if rows[0].get(c) is not None] or list(rows[0].keys())
        df = pd.DataFrame(rows)[show_cols]
        st.dataframe(df, use_container_width=True, hide_index=True)

    with card("Selected Artifact Details"):
        names = ["-- choose --"] + [r.get("artifact_name","(unnamed)") for r in rows]
        sel = st.selectbox("Artifact", names)
        if sel and sel != "-- choose --":
            rec = next((r for r in rows if r.get("artifact_name","(unnamed)") == sel), None)
            if rec:
                st.json(rec, expanded=True)
                links = []
                if rec.get("doc_url"):      links.append(f"[📘 Documentation]({rec['doc_url']})")
                if rec.get("download_url"): links.append(f"[⬇️ Download]({rec['download_url']})")
                if links: st.markdown(" • ".join(links))
