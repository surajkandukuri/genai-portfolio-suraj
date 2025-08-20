import os, json, streamlit as st
st.set_page_config(page_title="ProvisionalSuccessfulTestAgent", page_icon="✅", layout="centered")
st.title("✅ ProvisionalSuccessfulTestAgent")
try:
    data = json.loads(open("provisional_success_manifest.json").read())
    st.subheader("Workspace Manifest"); st.json(data, expanded=False)
except Exception as e:
    st.warning(f"Manifest not found: {e}")
st.write("Zero-key demo: environment is alive.")
st.caption(f"TEAM_ENV={os.getenv('TEAM_ENV')} • TABLE_PREFIX={os.getenv('TABLE_PREFIX')} • STORAGE_PREFIX={os.getenv('STORAGE_PREFIX')}")
gw = os.getenv("LLM_GATEWAY_URL") or "(disconnected)"
st.caption(f"LLM_GATEWAY_URL={gw}")
