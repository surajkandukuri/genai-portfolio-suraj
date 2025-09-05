# pages/2_admin.py
from __future__ import annotations
import os
import requests
import streamlit as st
from provisioning.theme import page_setup, page_header
from provisioning.autostart_api import ensure_fastapi

import streamlit as st
from supabase import create_client, Client  # keep if you use Supabase later

from provisioning.theme import page_header
from provisioning.ui import card

#st.set_page_config(page_title="Console", layout="wide")
page_header("ADMIN — CONSOLE", "Runtime checks, gateway status & post-provision agents")

'''
from provisioning.ui import card


# Bootstrap page (theme + sidebar)
page_setup(active="Console")
page_header("ADMIN — CONSOLE", "Runtime checks, gateway status & post-provision agents.")
'''
# Determine API base URL from the autostart info (or env override)
api = ensure_fastapi()  # cached; won't start twice
base_url = os.getenv("PA_API_BASE_URL") or api.get("url") or "http://127.0.0.1:7000"

def get_json(path: str, params: dict | None = None):
    url = f"{base_url}{path}"
    try:
        r = requests.get(url, params=params, timeout=5)
        r.raise_for_status()
        return True, r.json()
    except requests.exceptions.HTTPError as e:
        return False, f"HTTP Error {r.status_code}: {r.text or e}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

# Row 1: Health cards
c1, c2, c3 = st.columns(3)

with c1:
    with card("Fast API Gateway Health"):
        if st.button("Gateway Health"):
            ok, data = get_json("/gateway/health")
            st.success(data) if ok else st.error(data)

with c2:
    with card("Checks Agent Health"):
        if st.button("Checks Agent Health"):
            ok, data = get_json("/checks/health")
            st.success(data) if ok else st.error(data)

with c3:
    with card("Post Provision Sample LLM Agent"):
        if st.button("Try"):
            st.caption("Checking agent…")
            ok, data = get_json("/agents/postprovision/try")
            st.success(data) if ok else st.error(data)

st.markdown("---")

# Row 2: Sample generator
with card("Post Provision Sample LLM Agent"):
    cuisine = st.selectbox("Cuisine", ["Indian", "Italian", "Mexican", "Japanese"])
    if st.button("Generate Sample"):
        ok, data = get_json("/agents/sample-menu", params={"cuisine": cuisine})
        if ok:
            st.write(data)
            st.code("\n".join(f"- {x}" for x in data.get("items", [])))
        else:
            st.error(data)
