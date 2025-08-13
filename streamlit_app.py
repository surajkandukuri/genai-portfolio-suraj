# streamlit_app.py
import streamlit as st

st.set_page_config(page_title="AI Environment Provisioning Portal", page_icon="🧭", layout="centered")

st.title("🧭 AI Environment Provisioning Portal")
st.write("Use the left sidebar to navigate:")
st.markdown("- **Provision**: create selections and generate per-team workspaces\n- **Admin**: centralized agents (health/run), visible only to `centralized_uname`")
st.info("Pages are listed in the sidebar. If you don't see them, ensure the folder is named **pages/** and files are present.")
