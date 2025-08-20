# provisionalagent_homepage.py
import streamlit as st
from provisioning.theme import page_setup, hero
from provisioning.ui import card

def main() -> None:
    # Theme + sidebar (active highlights "Home")
    page_setup(active="Home")

    # Landing hero (matches your mock)
    hero(
        title_html="AI ENVIRONMENT<br/>PROVISIONING PORTAL",
        tagline="One-click, multi-team AI environment setup",
        cta_text="Provision Now",
        cta_page="pages/1_provision.py",  # must match your actual Provision page path
    )

    # Two info cards
    col1, col2 = st.columns(2)
    with col1:
        with card("Problem Statement"):
            st.markdown(
                "• Multiple teams, fragmented stacks, manual provisioning  \n"
                "• Lack of standardized governance and audit gaps  \n"
                "• “It’s all in Docker” visibility → need a human-readable map"
            )
    with col2:
        with card("What This Tool Solves"):
            st.markdown(
                "• One-click standardized environments with pre-approved options  \n"
                "• Built-in governance: policy packs, RBAC, audit trails"
            )

    # Feature trio
    f1, f2, f3 = st.columns(3)
    with f1:
        st.subheader("🚀 Rapid Deployment");  st.caption("From request to production in minutes.")
    with f2:
        st.subheader("🛡️ Governance-Ready"); st.caption("Approvals, policy packs, auditability.")
    with f3:
        st.subheader("🌿 Full Visibility");   st.caption("Track deployments by lean app status.")

if __name__ == "__main__":
    main()
