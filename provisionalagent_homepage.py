import streamlit as st

# Walkthrough (safe if helper not present)
try:
    from portfolio_walkthrough import mount, anchor
except Exception:
    def mount(*a, **k): ...
    def anchor(*a, **k): ...

# st.set_page_config(page_title="ProvisionAgent", layout="wide")

# Disable the Start Walkthrough button here (hover tips can still show)
mount("provision_landing", show_tour_button=False)
anchor("hero-provision")

st.title("AI ENVIRONMENT PROVISIONING PORTAL")
st.caption("One-click, multi-team AI environment setup")

anchor("btn-provision-now")
if st.button("Provision Now", use_container_width=False):
    st.switch_page("pages/1_provision.py")

left, right = st.columns(2)
with left:
    st.subheader("Problem Statement")
    st.markdown(
        "- Multiple teams, fragmented stacks, manual provisioning\n"
        "- Lack of standardized governance and audit gaps\n"
        "- “It’s all in Docker” visibility needing a human-readable map"
    )
with right:
    st.subheader("What This Tool Solves")
    st.markdown(
        "- One-click standardized envs with pre-approved options\n"
        "- Built-in governance: policy packs, RBAC, audit trails"
    )

st.divider()
c1, c2, c3 = st.columns(3)
c1.markdown("### 🚀 Rapid Deployment\nFrom request to production in minutes.")
c2.markdown("### 🛡️ Governance-Ready\nApprovals, policy packs, auditability.")
c3.markdown("### 🌿 Full Visibility\nTrack deployments by lean app status.")

st.divider()

# Make expander headers look like the section sub-headers
st.markdown(
    """
    <style>
      /* Global: style all expanders on this page */
      div[data-testid="stExpander"] > details > summary {
        font-size: 1.25rem;      /* ~H3 */
        font-weight: 700;        /* bold like card titles */
        line-height: 1.4;
        font-family: inherit;    /* matches your app font */
        padding-top: .15rem;
        padding-bottom: .15rem;
      }
    </style>
    """,
    unsafe_allow_html=True,
)


# After the 3 feature columns
with st.expander("Why this is Agentic AI", expanded=True):
    st.markdown("""
**One-liner:** You give a goal. The agent plans, uses tools, verifies, and learns.

**What makes it agentic**
- **Goal-driven planning** (decompose steps dynamically)
- **Tool use at runtime** (Supabase, Docker, FS, hashing, signing)
- **Closed-loop verification** (checks, retries, adaptation)
- **Memory & context** (preferences, ports, duplicates)
- **Policy-aware guardrails** (RBAC, allow-lists, approvals)
- **Explainable outputs** (manifest & audit)

**What it’s not**
- Static form → template, or a single scripted CI job.

**Example loop:** Plan → Act → Verify → Learn (with retries & guardrails).
    """)
