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

def _inject_expander_h3_style_once():
    if "_agentic_css_kpi" in st.session_state:
        return
    st.session_state["_agentic_css_kpi"] = True
    st.markdown(
        """
        <style>
          /* Make expander titles look like section headers */
          details > summary {
            font-size: 1.15rem !important;   /* close to your little headers */
            font-weight: 700 !important;
            letter-spacing: .2px;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

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

st.divider()

_inject_expander_h3_style_once()
with st.expander("Why this is Agentic AI", expanded=False):
    st.markdown(
        """
**One-liner:** The agent **reads dashboards like a human**, extracts KPI series, and **cross-checks across BI tools**—autonomously.

**Agent loop:** **Plan → Capture → Parse → Compare → Explain**

- **Goal-driven planning**  
  Given BI report links, the agent decides how to authenticate, when the page is ready, and what to capture (full views and relevant visuals).

- **Tool use at runtime (abstracted)**  
  It navigates to the reports, captures what’s on screen, performs **visual and structural extraction** to lift titles/labels/values, and **normalizes** them into tidy KPI rows. Artifacts are kept in **secure storage** for audit and replay.

- **Closed-loop verification**  
  Built-in **quality checks**, **retries/backoff**, and **statistical similarity tests** ensure reliability and flag likely drift.

- **Human-in-the-Loop (HITL) mapping**  
  When the agent is unsure or drift is flagged, users can **pair two widgets** (e.g., from different BI tools) and mark them as **equivalent** or **overwrite labels/units/aggregation**.  
  The decision is saved as a **contextual mapping (aliases + units + aggregations)** the agent **learns from** and **reuses** on future runs.

- **Memory & context(In the Near Future)**  
  Maintains a lightweight **KPI glossary** and **alias mapping**, along with unit/aggregation context and prior comparisons to improve over time.

- **Policy-aware guardrails**  
  Domain allow-lists, rate limiting, and a full **audit trail** of who/when/what was scanned keep it enterprise-safe.

- **Explainable outputs**  
  Produces **visual evidence**, the **normalized data**, and a **drift summary** (verdict + key metrics) so humans can verify the reasoning.
        """
    )
