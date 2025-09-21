# pages/2_admin.py
from __future__ import annotations

import os
import requests
import streamlit as st

from provisioning.theme import page_header
from provisioning.ui import card
from provisioning.autostart_api import ensure_fastapi

# Walkthrough (tooltips) helpers
import portfolio_walkthrough as wt

# ── Header ────────────────────────────────────────────────────────────────────
# st.set_page_config(page_title="Console", layout="wide")
page_header("ADMIN — CONSOLE", "Runtime checks, gateway status & post-provision agents")

# ── Walkthrough: register tooltips for this page and mount (no tour button) ───
wt.register(
    "admin_console",
    tips={
        # Card 1 – Gateway
        "card-gw": "Pings the FastAPI Gateway and shows status/latency. Confirms routes & auth are reachable.",
        "btn-gw": "Calls the gateway `/health` endpoint and displays the response.",
        # Card 2 – Checks Agent
        "card-checks": "Runs post-provision checks (secrets, buckets, DB). Returns a concise health summary.",
        "btn-checks": "Executes the Checks Agent health call and surfaces issues first.",
        # Card 3 – Sample LLM Agent (quick E2E proof)
        "card-sample": "Tiny demo agent behind the gateway; verify inference end-to-end.",
        "btn-sample": "Generates a quick sample so you can visually confirm responses.",
        # Row 2 – Generator
        "card-generator": "Sample content generator that exercises the same gateway path as real agents.",
        "sel-cuisine": "Pick a cuisine to generate an example menu (simple, readable output).",
        "btn-generate": "Calls `/agents/sample-menu` and prints the response + a friendly list.",
    },
)
wt.mount("admin_console", show_tour_button=False)

# ── API base URL (autostart) ──────────────────────────────────────────────────
api = ensure_fastapi()  # cached; won't start twice
base_url = os.getenv("PA_API_BASE_URL") or api.get("url") or "http://127.0.0.1:7000"

def get_json(path: str, params: dict | None = None):
    url = f"{base_url}{path}"
    try:
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        return True, r.json()
    except requests.exceptions.HTTPError as e:
        # r is still in scope here
        return False, f"HTTP Error {r.status_code}: {r.text or e}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

# ── Row 1: Health cards ───────────────────────────────────────────────────────
c1, c2, c3 = st.columns(3)

with c1:
    with card("Fast API Gateway Health"):
        wt.anchor("card-gw")
        if st.button("Gateway Health"):
            wt.anchor("btn-gw")
            ok, data = get_json("/gateway/health")
            st.success(data) if ok else st.error(data)

with c2:
    with card("Checks Agent Health"):
        wt.anchor("card-checks")
        if st.button("Checks Agent Health"):
            wt.anchor("btn-checks")
            ok, data = get_json("/checks/health")
            st.success(data) if ok else st.error(data)

with c3:
    with card("Post Provision Sample LLM Agent"):
        wt.anchor("card-sample")
        if st.button("Try"):
            wt.anchor("btn-sample")
            st.caption("Checking agent…")
            ok, data = get_json("/agents/postprovision/try")
            st.success(data) if ok else st.error(data)

st.markdown("---")

# ── Row 2: Sample generator (exercises gateway path) ──────────────────────────
with card("Post Provision Sample LLM Agent"):
    wt.anchor("card-generator")
    cuisine = st.selectbox("Cuisine", ["Indian", "Italian", "Mexican", "Japanese"])
    wt.anchor("sel-cuisine")
    if st.button("Generate Sample"):
        wt.anchor("btn-generate")
        ok, data = get_json("/agents/sample-menu", params={"cuisine": cuisine})
        if ok:
            st.write(data)
            items = data.get("items", [])
            if items:
                st.code("\n".join(f"- {x}" for x in items))
        else:
            st.error(data)
