import streamlit as st

st.set_page_config(page_title="GenAI Portfolio", layout="wide")

# ---- Landing page content for the portfolio home ----
def landing():
    st.title("AI AGENTS")
    st.subheader("About Me")
    st.write(
        "I’m a software engineer specializing in AI agent development. "
        "I create intelligent agents that can automate tasks, provide insights, "
        "and enhance productivity."
    )

# ---- Pages ----
home       = st.Page(landing,                        title="Landing Page",    icon=":material/home:", default=True)

# ProvisionAgent group
prov_agent = st.Page("provisionalagent_homepage.py",   title="ProvisionAgent",  icon=":material/engineering:")
provision  = st.Page("pages/1_provision.py",         title="Provision",       icon=":material/rocket_launch:")

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
        "Admin · ProvisionalAgent": [console, reports, artifacts],
        "Account": [logout],
    },
    position="sidebar",
)

nav.run()
