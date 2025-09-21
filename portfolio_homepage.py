import streamlit as st
import sys, asyncio, platform

# --- Page config: exactly once, before any UI calls ---
st.set_page_config(page_title="GenAI Portfolio", page_icon="🧠", layout="wide", initial_sidebar_state="expanded")

# Completely disable vertical page scroll (keep sidebar scrollable)
st.markdown("""
<style>
html, body, [data-testid="stAppViewContainer"] {
  height: 100vh;
  overflow: hidden !important; /* no page scroll */
}
[data-testid="stSidebar"], [data-testid="stSidebarContent"] {
  overflow: auto !important;   /* allow sidebar to scroll if long */
}
</style>
""", unsafe_allow_html=True)

# Windows asyncio policy (safe no-op on non-Windows)
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

# ---- Landing page content for the portfolio home ----
def landing():
    st.title("AI AGENTS")
    st.caption("These are implementable AI agents — designed to work today and easily extend into production-grade systems.")

    st.markdown("""
    ## Solving Common Enterprise Data Challenges
    In modern data and BI environments, the same issues keep showing up: fragmented tech stacks, conflicting KPIs, inconsistent queries, and lack of standardization. These slow decisions, increase cost, and limit scale.
    With deep experience across the data lifecycle, I build agentic AI systems that tackle these problems head-on—fast, intelligently, and with measurable business value.
    """)

    # First row
    col1, col2 = st.columns(2, gap="small")

    with col1:
        st.subheader("🚀 Provisioning Agent")
        st.markdown(
            "Enables centralized teams to define, manage, and enforce environment standards. "
            "It generates Docker-based config files so teams can spin up consistent, self-contained ecosystems on demand—at any scale."
            "<br/><span style='color:#5f6ad2'><em>Guided by agentic AI that adapts to context and usage—beyond static templates.</em></span>",
            unsafe_allow_html=True
        )
        with st.expander("Built For", expanded=False):
            st.markdown("""
            - 📊 Business leaders who want governance without blocking delivery.  
            - 🧠 AI practitioners building dynamic, policy-driven environments.  
            - ⚙️ Engineers who value reusable infra scaffolding.  
            - 🧑‍💼 Capability reviewers evaluating GenAI + platform automation experience.
            """)

    with col2:
        st.subheader("📊 KPI Drift Hunter")
        st.markdown(
            "Flags KPI inconsistencies across BI tools via automated capture and comparison. "
            "Detects drift in seconds—making issues visible, actionable, and easier to resolve."
            "<br/><span style='color:#5f6ad2'><em>Uses agentic checks that interpret visual and semantic signals—not just rule-based diffs.</em></span>",
            unsafe_allow_html=True
        )
        with st.expander("Built For", expanded=False):
            st.markdown("""
            - 📊 Stakeholders relying on consistent, cross-platform reporting.  
            - 🧠 Builders designing intelligent validation pipelines.  
            - ⚙️ Devs maintaining aligned definitions across tools.  
            - 🧑‍💼 Reviewers assessing impact of applied GenAI in analytics.
            """)

    # tighten space between rows
    st.markdown("<div style='margin-top:-1rem;'></div>", unsafe_allow_html=True)

    # Second row
    col3, col4 = st.columns(2, gap="small")

    with col3:
        st.subheader("🧠 Query Pattern Analyzer *(In Progress)*")
        st.markdown(
            "Analyzes query logs to uncover inefficiencies and high-frequency patterns. "
            "Enables centralized teams to adapt warehouse structures—like summary tables or rollups—to match how data is actually used."
            "<br/><span style='color:#5f6ad2'><em>Applies agentic pattern recognition across teams and time—no manual rule tuning.</em></span>",
            unsafe_allow_html=True
        )
        with st.expander("Built For", expanded=False):
            st.markdown("""
            - 📊 Data owners managing cost and performance tradeoffs.  
            - 🧠 Analysts and architects automating pattern recognition.  
            - ⚙️ Platform teams tuning storage and compute models.  
            - 🧑‍💼 Leadership or reviewers looking at usage-informed design thinking.
            """)

    with col4:
        st.subheader("🎨 BI Standards Enforcer *(In Progress)*")
        st.markdown(
            "Scans dashboards for design inconsistencies across BI tools. "
            "Surfaces violations of UI standards and can be run on-demand to support visual governance at scale."
            "<br/><span style='color:#5f6ad2'><em>Performs agentic review of layout and metadata and adapts as standards evolve.</em></span>",
            unsafe_allow_html=True
        )
        with st.expander("Built For", expanded=False):
            st.markdown("""
            - 📊 Execs and stakeholders who expect dashboard consistency.  
            - 🧠 Designers and builders exploring rule-driven UI enforcement.  
            - ⚙️ BI teams tired of manual UI audits.  
            - 🧑‍💼 Reviewers measuring end-user experience at scale.
            """)

    st.markdown("<div style='margin-top:1rem;'></div>", unsafe_allow_html=True)
    st.info("Tip: Each page in the left nav is a demo scenario. Open a report link, click **Run**, and inspect stored artifacts.")

# ---- Pages ----
home       = st.Page(landing,                              title="Landing Page",    icon=":material/home:", default=True)

# ProvisionAgent group
prov_agent = st.Page("provisionalagent_homepage.py",       title="ProvisionAgent",  icon=":material/engineering:")
provision  = st.Page("pages/1_provision.py",               title="Provision",       icon=":material/rocket_launch:")

# KPI Drift Hunter Agent group
kpi_drift  = st.Page("kpidrifthunteragent_homepage.py",    title="KPI Drift Hunter",      icon=":material/analytics:")
kpi_drift_runandextract   = st.Page("pages/23_kpidrift_runandextract.py",  title="Run and Extract",  icon=":material/play_circle:")
kpi_drift_parseandcompare = st.Page("pages/24_kpidrift_parseandcompare.py",title="Parse and Compare",icon=":material/compare_arrows:")
kpi_drift_report          = st.Page("pages/26_kpidrift_reports.py",        title="Report",           icon=":material/insights:")
kpi_drift_documentation         = st.Page("pages/25_kpidrift_psuedocode.py",        title="PsuedoCode",           icon=":material/library_books:")

# Admin children
console    = st.Page("pages/2_admin.py",                   title="Console",         icon=":material/terminal:")
reports    = st.Page("pages/3_Reports.py",                 title="Reports",         icon=":material/insights:")
artifacts  = st.Page("pages/4_Artifacts.py",               title="Artifacts",       icon=":material/archive:")
psuedocode = st.Page("pages/5_psuedocode.py", title="PsuedoCode",  icon=":material/library_books:")
#Certificates 
certificates_learning = st.Page("pages/55_certificates_learning.py",title="Certificates", icon=":material/workspace_premium:")
# Account
logout     = st.Page("pages/9_Logout.py",                  title="Logout",          icon=":material/logout:")

# ---- Navigation tree ----
nav = st.navigation(
    {
        "Home": [home],
        "ProvisionAgent": [prov_agent, provision,console, reports, artifacts,psuedocode],
        "KPI Drift Hunter Agent": [kpi_drift, kpi_drift_runandextract, kpi_drift_parseandcompare, kpi_drift_report,kpi_drift_documentation],
        #"Admin · ProvisionalAgent": [console, reports, artifacts],
        "Certificates":[certificates_learning],
        "Account": [logout],
    },
    position="sidebar",
)

nav.run()
