# streamlit_app.py
import streamlit as st
from provisioning.ui import inject_styles, render_sidebar, card

st.set_page_config(page_title="AI Environment Provisioning Portal", page_icon="🧭", layout="centered")
inject_styles()
render_sidebar("Home")

# HERO
st.markdown(
    """
<div class="hero">
  <h1>AI Environment Provisioning Portal</h1>
  <div class="tagline">One-Click, Multi-Team AI Environment Setup</div>
</div>
""",
    unsafe_allow_html=True,
)

# CTA card (top banner)
with card("Reduce deployment time from weeks to minutes",
          "with centralized governance, standardized templates, and audit-ready artifacts"):
    if st.button("Provision Now"):
        # Available on modern Streamlit; if older, fallback to sidebar link
        try:
            st.switch_page("pages/1_Provision.py")
        except Exception:
            st.success("Use the sidebar → Provision")

# Two info columns like your slide
c1, c2 = st.columns(2)
with c1:
    with card("Problem Statement"):
        st.write(
            "• Multiple teams, fragmented stacks, manual provisioning\n"
            "• Lack of standardized governance & approvals\n"
            "• Unclear ownership & audit gaps\n"
            "• “It’s all in Docker” visibility → need human-readable map"
        )
with c2:
    with card("What This Tool Solves"):
        st.write(
            "• One-click standardized environments with pre-approved options\n"
            "• Built-in governance: policy packs, RBAC, audit trails\n"
            "• Deployed artifacts view shows versions/configs/status"
        )

# Feature tiles row
tc1, tc2, tc3 = st.columns(3)
with tc1:
    with card("🚀 Rapid Deployment"):
        st.caption("From request to production in minutes.")
with tc2:
    with card("🛡️ Governance-Ready"):
        st.caption("Approvals, policy packs, auditability.")
with tc3:
    with card("📈 Full Visibility"):
        st.caption("Track deployments by team app status.")
