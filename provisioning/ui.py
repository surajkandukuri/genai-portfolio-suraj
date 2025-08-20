# provisioning/ui.py
from __future__ import annotations
import streamlit as st
from contextlib import contextmanager
from .menu import MENU

def inject_styles() -> None:
    """Base styles shared by all pages (sidebar + headers inside cards)."""
    st.markdown(
        """
        <style>
          /* Card header/subtitle (the box is provided by st.container(border=True)) */
          .pa-card-header { font-weight: 700; margin: .15rem 0 .35rem; }
          .pa-card-sub { color: var(--pa-muted); font-size:.95rem; margin-top:-.2rem; margin-bottom:.35rem; }

          /* Sidebar look */
          [data-testid="stSidebar"] { padding-top: .8rem; }
          .pa-menu h3 {
            margin: 0 0 .75rem; font-size: 1rem; letter-spacing:.02em;
            text-transform: uppercase; opacity:.7;
          }
          .pa-menu .section { margin-top: .5rem; margin-bottom: .25rem; font-weight: 700; }

          /* Page links: rounded, with a clear active state (disabled=True) */
          [data-testid="stPageLinkContainer"] > a,
          [data-testid="stPageLinkContainer"] > button { border-radius: 10px; }
          [data-testid="stPageLinkContainer"] > a[aria-disabled="true"],
          [data-testid="stPageLinkContainer"] > button[disabled]{
              background: var(--pa-primary) !important;
              color: #fff !important;
              opacity: 1 !important;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

@contextmanager
def card(title: str, subtitle: str | None = None, *, border: bool = True):
    """
    Consistent panel used across pages.
    Uses Streamlit's bordered container to avoid stray empty <div>s.
    """
    with st.container(border=border):
        st.markdown(f'<div class="pa-card-header">{title}</div>', unsafe_allow_html=True)
        if subtitle:
            st.markdown(f'<div class="pa-card-sub">{subtitle}</div>', unsafe_allow_html=True)
        yield

def render_sidebar(active: str) -> None:
    """Build the left menu from MENU and highlight the active leaf."""
    with st.sidebar:
        st.markdown('<div class="pa-menu"><h3>Portal Menu</h3></div>', unsafe_allow_html=True)

        for item in MENU:
            label = item["label"]
            path = item.get("path")
            children = item.get("children", [])

            if path is None and children:
                # Section header (e.g., "Admin")
                st.markdown(f'<div class="section">{label}</div>', unsafe_allow_html=True)
                for child in children:
                    clabel, cpath = child["label"], child["path"]
                    st.page_link(cpath, label=clabel, disabled=(active == clabel))
                st.markdown("")  # spacer
            else:
                # Top-level link (Home, Provision, Logout, etc.)
                st.page_link(path, label=label, disabled=(active == label))
