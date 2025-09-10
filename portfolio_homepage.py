import streamlit as st
import sys, asyncio, platform

# --- Page config: exactly once, before any UI calls ---
st.set_page_config(page_title="GenAI Portfolio", page_icon="🧠", layout="wide", initial_sidebar_state="expanded")

# Windows asyncio policy (safe no-op on non-Windows)
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

# ---- Landing page content for the portfolio home ----
def landing():
    st.title("AI AGENTS")
    st.caption("Cross-Platform BI • Provisioning • Agentic Workflows • Enterprise Data Governance")

    st.markdown("""
    ## About Me
    I’m a **Data & BI Architect with hands on Programming skillset** turned **GenAI Solutions Architect**. I build agentic systems that solve real-world,
    enterprise-grade problems—fast. My portfolio showcases Agentic AI solutions to real world enterprise problems, which are 
                 
        - ** Different Teams Different Tech Stacks hard to standardized : Provisional Agent 
        - ** Different Teams Different BI Tools hard to Achieve Single Version of Truth because definitions can and will drift : KPI Drift Hunter

    ### What I optimize for
    - **Speed with guardrails**: Playwright automation + Supabase Storage/DB + RLS-friendly patterns.
    - **Repeatability**: “Diagrams as code,” infra as config, deterministic folder/layout conventions.
    - **Observability**: hashes, manifests, and DB facts for traceability & audits.
    """)

    st.divider()

    c1, c2 = st.columns(2, gap="large")

    with c1:
        st.subheader("🚀 Provisioning Agent")
        st.markdown("""
    **Problem:** Centralized teams often struggle to ensure consistent technology stacks and environment standards across departments.  
    **Solution:** A self-service portal, governed by the central team, that provisions dev/qa/prod workspaces along with secrets, storage buckets, and database artifacts in a standardized manner.

    **Highlights**
    - Predefined templates for teams and environments (with opinionated defaults).  
    - Automated secrets injection (service-role vs anon keys).  
    - Administrative pages for **Console**, **Reports**, and **Artifacts**.  

    **Try it**
    - **ProvisionAgent → Provision** (create a new environment)  
    - **Admin → Console / Reports / Artifacts** (review provisioned outputs)  
        """)

    with c2:
        st.subheader("📊 KPI Drift Hunter")
        st.markdown("""
    **Problem:** The same KPI can appear inconsistent across platforms (e.g., Power BI vs Tableau).  
    **Solution:** An agentic pipeline that automates **Capture → Extract → Persist → Compare**, ensuring KPI alignment across BI tools.

    **Highlights**
    - Headless capture (full-page and per-widget crops).  
    - Automated storage in Supabase (**kpidrifthunter** bucket).  
    - Database facts (`kdh_screengrab_dim`, `kdh_widget_dim`) with SHA-256 deduplication.  
    - Designed for OCR and shape-matching extensions to detect KPI drift.  

    **Try it**
    - **KPI Drift Hunter → Run and Extract** (process public URLs)  
    - **KPI Drift Hunter → Parse and Compare** (coming soon in V1)  
        """)

    st.divider()
    st.subheader("Tech Stack at a Glance")
    st.markdown("""
    **Python, Playwright, Streamlit, Supabase (Storage + Postgres/RLS), Pandas, Mermaid, Diagrams-as-Code**  
    **Next Up:** OCR (Tesseract/PaddleOCR), similarity checks (DTW/correlation), and glossary-linked KPI definitions.  
    """)

    st.info("Tip: Each page in the left nav is a demo scenario. Open a report link, click **Run**, and inspect stored artifacts.")

# ---- Pages ----
home       = st.Page(landing,                              title="Landing Page",    icon=":material/home:", default=True)

# ProvisionAgent group
prov_agent = st.Page("provisionalagent_homepage.py",       title="ProvisionAgent",  icon=":material/engineering:")
provision  = st.Page("pages/1_provision.py",               title="Provision",       icon=":material/rocket_launch:")

# KPI Drift Hunter Agent group
kpi_drift  = st.Page("kpidrifthunteragent_homepage.py",    title="KPI Drift Hunter",      icon=":material/analytics:")
kpi_drift_runandextract   = st.Page("pages/23_kpidrift_runandextract.py",  title="Run and Extract",  icon=":material/play_circle:")
kpi_drift_parseandcompare = st.Page("pages/24_kpidrift_parseandcompare.py",title="Parse and Compare",icon=":material/play_circle:")
kpi_drift_report          = st.Page("pages/26_kpidrift_reports.py",        title="Report",           icon=":material/insights:")

# Admin children
console    = st.Page("pages/2_admin.py",                   title="Console",         icon=":material/terminal:")
reports    = st.Page("pages/3_Reports.py",                 title="Reports",         icon=":material/insights:")
artifacts  = st.Page("pages/4_Artifacts.py",               title="Artifacts",       icon=":material/archive:")

# Account
logout     = st.Page("pages/9_Logout.py",                  title="Logout",          icon=":material/logout:")

# ---- Navigation tree ----
nav = st.navigation(
    {
        "Home": [home],
        "ProvisionAgent": [prov_agent, provision],
        "KPI Drift Hunter Agent": [kpi_drift, kpi_drift_runandextract, kpi_drift_parseandcompare, kpi_drift_report],
        "Admin · ProvisionalAgent": [console, reports, artifacts],
        "Account": [logout],
    },
    position="sidebar",
)

nav.run()
