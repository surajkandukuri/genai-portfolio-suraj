import streamlit as st

#st.set_page_config(page_title="KPI Drift Hunter", layout="wide")

st.title("KPI Drift Hunter Agent")
st.caption("One-click, multi-team KPI Drift hunter across BI Platforms")

# CTA → go to pages/1_provision.py
if st.button("Run the Scan", use_container_width=False):
    st.switch_page("pages/21_kpidrift_runthescan.py")

# CTA → go to pages/1_provision.py
if st.button("Extract Widgets", use_container_width=False):
    st.switch_page("pages/22_kpidrift_widgetextractor.py")
# CTA → go to pages/1_provision.py
if st.button("Run and Extract Widgets", use_container_width=False):
    st.switch_page("pages/23_kpidrift_runandextract.py")



# (Your cards/sections)
left, right = st.columns(2)
with left:
    st.subheader("Problem Statement")
    st.markdown(
        "- Multiple teams, fragmented BI Platforms,Potentially different KPIs\n"
        "- Lack of clear view of KPI Definition Drifts\n"
        "- Compromised Single Version of Truth\n"

        
    )
with right:
    st.subheader("What This Tool Solves")
    st.markdown(
        "- One‑click Agentic AI that swims through BI ecosystems \n"
        "- Need for a unified tool to track and manage KPI drifts across platforms"
    )

st.divider()
c1, c2, c3 = st.columns(3)
c1.markdown("### 🚀 Enterprise Grade\nAbility to connect to multiple BI Ecosystems")
c2.markdown("### 🛡️ Single Stop to see KPIs and their Drifts among BI Products")

