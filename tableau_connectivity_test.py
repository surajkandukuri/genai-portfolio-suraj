import streamlit as st
import tableauserverclient as TSC

# Read from .streamlit/secrets.toml
server_url = st.secrets["TABLEAU_SERVER_URL"]
site_id    = st.secrets["TABLEAU_SITE_ID"]
username   = st.secrets["TABLEAU_USERNAME"]
password   = st.secrets["TABLEAU_PASSWORD"]

server = TSC.Server(server_url, use_server_version=True)
auth   = TSC.TableauAuth(username, password, site_id)

with server.auth.sign_in(auth):
    views, _ = server.views.get()
    print("Views you can export:")
    for v in views[:5]:
        print("-", v.name, v.id)
