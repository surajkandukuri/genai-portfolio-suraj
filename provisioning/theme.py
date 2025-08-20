# provisioning/theme.py
from __future__ import annotations
import streamlit as st
import os 
from provisioning.ui import inject_styles as base_styles, render_sidebar
from provisioning.autostart_api import ensure_fastapi

# ---- Theme tokens (edit here to restyle the whole app) -----------------------
THEME = {
    "font_family": "Inter, system-ui, -apple-system, Segoe UI, Roboto",
    "bg": "#F3F4F6",            # app background
    "panel": "#FFFFFF",         # card background
    "primary": "#10B981",       # CTA green (landing mock)
    "text": "#0F172A",
    "muted": "#64748B",
    "radius": "12px",
    "shadow": "0 4px 18px rgba(2, 6, 23, 0.06)",
}

def _inject_theme_css() -> None:
    """Define global CSS variables + primitives, then load base component styles."""
    # 1) Design tokens & page primitives
    st.markdown(
        f"""
        <style>
          :root {{
            --pa-bg: {THEME['bg']};
            --pa-panel: {THEME['panel']};
            --pa-primary: {THEME['primary']};
            --pa-text: {THEME['text']};
            --pa-muted: {THEME['muted']};
            --pa-radius: {THEME['radius']};
            --pa-shadow: {THEME['shadow']};
            --pa-font: {THEME['font_family']};
          }}
          html, body, [data-testid="stAppViewContainer"] {{
            background: var(--pa-bg) !important;
            color: var(--pa-text);
            font-family: var(--pa-font);
          }}
          /* Primary buttons */
          .stButton > button {{
            background: var(--pa-primary);
            color:#fff; border:0; border-radius: var(--pa-radius);
            padding:.6rem 1rem; font-weight:600;
          }}
          /* Hero + headers (landing/page headers) */
          .pa-hero h1 {{
            font-size:3.2rem; line-height:1.05; font-weight:800; letter-spacing:.01em;
            text-transform:uppercase; margin:0;
          }}
          .pa-hero .tagline {{ margin-top:.35rem; font-size:1.05rem; color:var(--pa-muted); }}
          .pa-header h1 {{
            font-size:2.0rem; line-height:1.1; font-weight:800; text-transform:uppercase;
            margin:.25rem 0; word-break:keep-all; white-space:nowrap;
          }}
          .pa-header .tag {{ opacity:.75; margin-top:.15rem; }}
        </style>
        """,
        unsafe_allow_html=True,
    )
    # 2) Shared component styles (cards, sidebar, links)
    base_styles()

def page_setup(active: str, page_title: str = "AI Environment Provisioning Portal") -> None:
    st.set_page_config(page_title=page_title, page_icon="🧭", layout="centered")
    try:
        st.cache_data.clear(); st.cache_resource.clear()
    except Exception:
        pass
    _inject_theme_css()
    render_sidebar(active)

    # 🔌 Auto-start FastAPI (idempotent; cached)
    info = ensure_fastapi()
    if os.getenv("PA_API_SHOW_STATUS", "0").lower() in ("1", "true", "yes"):
        st.sidebar.caption(f"API: {info['status']} → {info['url'] or 'disabled'}")


def hero(title_html: str, tagline: str, cta_text: str | None = None, cta_page: str | None = None) -> None:
    """Landing hero block (HTML allowed in title_html for line breaks)."""
    st.markdown(
        f'<div class="pa-hero"><h1>{title_html}</h1><div class="tagline">{tagline}</div></div>',
        unsafe_allow_html=True,
    )
    if cta_text:
        if st.button(cta_text, key="pa-hero-cta") and cta_page:
            try:
                st.switch_page(cta_page)
            except Exception:
                st.success("Use the sidebar → Provision")

def page_header(title: str, tag: str = "") -> None:
    """Uniform page header for all non-landing pages."""
    st.markdown(
        f'<div class="pa-header"><h1>{title}</h1><div class="tag">{tag}</div></div>',
        unsafe_allow_html=True,
    )
