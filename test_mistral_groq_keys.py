# test_mistral_groq_keys.py
import streamlit as st
from mistralai import Mistral
from langchain_groq import ChatGroq

# --- Mistral test ---
m_key = st.secrets["MISTRAL_API_KEY"]
m_client = Mistral(api_key=m_key)
resp = m_client.chat.complete(
    model="mistral-small-latest",  # cheap + fast text-only model
    messages=[{"role": "user", "content": "Say 'ok'"}]
)
print("Mistral OK:", resp.choices[0].message.content[:60])

# --- Groq test ---
g_key = st.secrets["GROQ_API_KEY"]
llm = ChatGroq(model="llama-3.1-8b-instant", temperature=0.0)
out = llm.invoke("Say 'ok'")
print("Groq OK:", out.content)
