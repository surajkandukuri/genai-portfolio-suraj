import streamlit as st

st.set_page_config(page_title="ProvisionAgent", layout="wide")

st.title("AI ENVIRONMENT PROVISIONING PORTAL")
st.caption("One-click, multi-team AI environment setup")

# CTA → go to pages/1_provision.py
if st.button("Provision Now", use_container_width=False):
    st.switch_page("pages/1_provision.py")

# (Your cards/sections)
left, right = st.columns(2)
with left:
    st.subheader("Problem Statement")
    st.markdown(
        "- Multiple teams, fragmented stacks, manual provisioning\n"
        "- Lack of standardized governance and audit gaps\n"
        "- “It’s all in Docker” visibility needing a human‑readable map"
    )
with right:
    st.subheader("What This Tool Solves")
    st.markdown(
        "- One‑click standardized envs with pre‑approved options\n"
        "- Built‑in governance: policy packs, RBAC, audit trails"
    )

st.divider()
c1, c2, c3 = st.columns(3)
c1.markdown("### 🚀 Rapid Deployment\nFrom request to production in minutes.")
c2.markdown("### 🛡️ Governance‑Ready\nApprovals, policy packs, auditability.")
c3.markdown("### 🌿 Full Visibility\nTrack deployments by lean app status.")
