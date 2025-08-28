import streamlit as st
import sys, asyncio
import platform 

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

st.set_page_config(page_title="GenAI Portfolio", layout="wide")

# ---- Landing page content for the portfolio home ----
def landing():
    st.title("AI AGENTS")
    st.subheader("About Me")
    st.write(
        "I’m a veteran BI Full Stack Solution Provider, Implementor. "
        "These AI Agents are designed to streamline processes, me taking a stab at Agentic AI solving Real World Problems Enterprise Grade Problems"
        
    )

# ---- Pages ----
home       = st.Page(landing,                        title="Landing Page",    icon=":material/home:", default=True)

# ProvisionAgent group
prov_agent = st.Page("provisionalagent_homepage.py",   title="ProvisionAgent",  icon=":material/engineering:")
provision  = st.Page("pages/1_provision.py",         title="Provision",       icon=":material/rocket_launch:")

# KPI Drift Hunter Agent group
kpi_drift  = st.Page("kpidrifthunteragent_homepage.py", title="KPI Drift Hunter", icon=":material/analytics:")
# KPI Drift Hunter Agent child
kpi_drift_run = st.Page("pages/21_kpidrift_runthescan.py", title="Run the Scan", icon=":material/play_circle:") 
#KPI Widget Extractor child
kpi_drift_widgetextractor = st.Page("pages/22_kpidrift_widgetextractor.py", title="Widget Extractor", icon=":material/play_circle:") 

# Admin children
console    = st.Page("pages/2_admin.py",             title="Console",         icon=":material/terminal:")
reports    = st.Page("pages/3_Reports.py",           title="Reports",         icon=":material/insights:")
artifacts  = st.Page("pages/4_Artifacts.py",         title="Artifacts",       icon=":material/archive:")

# Account
logout     = st.Page("pages/9_Logout.py",            title="Logout",          icon=":material/logout:")

# ---- Navigation tree ----
nav = st.navigation(
    {
        "Home": [home],
        "ProvisionAgent": [prov_agent, provision],     # landing + child    
        "KPI Drift Hunter Agent": [kpi_drift, kpi_drift_run,kpi_drift_widgetextractor],  # landing + child
        "Admin · ProvisionalAgent": [console, reports, artifacts],
        "Account": [logout],
    },
    position="sidebar",
)

nav.run()
