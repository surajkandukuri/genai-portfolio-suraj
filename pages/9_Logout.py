# pages/9_Logout.py
import streamlit as st
from provisioning.ui import inject_styles, render_sidebar, card

st.set_page_config(page_title="Logout", page_icon="🚪", layout="centered")
inject_styles()
render_sidebar("Logout")

st.title("Logout")
st.caption("This will clear your session and return you to Home.")

with card("Logout"):
    if st.button("Sign out", key="logout_btn"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.success("Signed out. Use the sidebar to navigate.")
