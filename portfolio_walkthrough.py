# portfolio_walkthrough.py — robust tooltips via Tippy.js (hover + click)

from __future__ import annotations
import html
import json
import streamlit as st

# ---------- Public API ---------------------------------------------------------

def mount(page_key: str, *, show_tour_button: bool = False) -> None:
    """Mount controls and inject assets once. (Tour button optional; default off.)"""
    _ctx()["page_key"] = page_key
    _controls(show_tour_button)  # renders Show tips checkbox

    if not st.session_state.get("_wt_assets_injected", False):
        st.session_state["_wt_assets_injected"] = True
        st.markdown(_css(), unsafe_allow_html=True)
        st.markdown(_tippy_assets(), unsafe_allow_html=True)

    # Register tour steps placeholder (kept for API compat; unused here)
    st.markdown(_bootstrap_steps([]), unsafe_allow_html=True)

def anchor(id: str) -> None:
    """Render a small ⓘ with a native browser tooltip (robust in Streamlit)."""
    page_key = _ctx().get("page_key", "")
    tip = (REGISTRY.get(page_key, {}).get("tips", {}) or {}).get(id, "")
    if not tip:
        return
    show = st.session_state.get("_wt_show_tips", True)
    style = "" if show else "display:none;"
    # Browser-native tooltip via title= (no clipping, no custom JS/CSS needed)
    html_snippet = (
        f"<span id='wt-{id}' class='wt-info' title='{html.escape(tip, quote=True)}' "
        f"style='{style}'>ⓘ</span>"
    )
    st.markdown(html_snippet, unsafe_allow_html=True)


def register(page_key: str, *, tips: dict[str, str] | None = None) -> None:
    REGISTRY.setdefault(page_key, {})
    if tips:
        REGISTRY[page_key].setdefault("tips", {}).update(tips)

# ---------- Internals ----------------------------------------------------------

def _ctx() -> dict:
    if "_wt_ctx" not in st.session_state:
        st.session_state["_wt_ctx"] = {}
    return st.session_state["_wt_ctx"]

def _controls(show_tour_button: bool) -> None:
    if "_wt_show_tips" not in st.session_state:
        st.session_state["_wt_show_tips"] = True
    col1, _ = st.columns([1, 1])
    with col1:
        st.checkbox("💡 Show tips", key="_wt_show_tips")

def _css() -> str:
    return """
    <style>
      .wt-info{
        display:inline-block;
        margin-left:.35rem;
        padding:0 .2rem;
        font-weight:600;
        font-size:.90rem;
        line-height:1;
        color:#8a8f98;
        border-radius:4px;
        cursor:help;
        user-select:none;
      }
      .wt-info:hover{ color:#4b9fff; background:rgba(75,159,255,.07); }
    </style>
    """


def _tippy_assets() -> str:
    # Popper + Tippy from CDN, initialize on any .wt-info
    return """
    <link rel="stylesheet" href="https://unpkg.com/tippy.js@6/dist/tippy.css">
    <link rel="stylesheet" href="https://unpkg.com/tippy.js@6/themes/light-border.css">
    <script src="https://unpkg.com/@popperjs/core@2"></script>
    <script src="https://unpkg.com/tippy.js@6/dist/tippy.umd.min.js"></script>
    <script>
      function initWT(){
        if (!window.tippy) return;
        // Remove any existing instances then re-init (Streamlit reruns)
        document.querySelectorAll('.wt-info').forEach(function(el){
          if (el._tippy) { el._tippy.destroy(); }
        });
        tippy('.wt-info', {
          allowHTML: true,
          theme: 'light-border',
          maxWidth: 420,
          interactive: true,
          appendTo: () => document.body,   // render above Streamlit containers
          trigger: 'mouseenter click',
          placement: 'right',
          moveTransition: 'transform 0.15s ease-out',
          offset: [0, 8],
        });
      }
      // Initialize on load and on Streamlit DOM updates
      const ready = () => initWT();
      if (document.readyState !== 'loading') ready();
      else document.addEventListener('DOMContentLoaded', ready);
      // Streamlit re-renders: try again after small delays
      setTimeout(initWT, 250);
      setTimeout(initWT, 750);
    </script>
    """

def _bootstrap_steps(steps: list[dict]) -> str:
    return f"<script>window.__WT_STEPS__ = {json.dumps(steps)};</script>"

# ---------- Registry (your page keys & tips) ----------------------------------

REGISTRY = {
    "provision_agent": {
        "tips": {
            "overview-chooser": (
                "Pick your options to provision your environment: "
                "<strong>Environment</strong> (DEV/QA/PROD), "
                "<strong>Core programming/runtime</strong>, "
                "<strong>Database</strong>, <strong>Secrets Manager</strong>, "
                "and <strong>Storage</strong>."
            )
        }
    },
    "provision_landing": {
        "tips": {
            "hero-provision": "Overview of standardized, one-click environment setup.",
            "btn-provision-now": "Jump into the detailed page to choose env, stack and artifacts.",
        }
    },
}
