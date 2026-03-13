import streamlit as st
import requests
import pandas as pd

API_URL = "http://localhost:8000"

st.set_page_config(page_title="SF Intelligence Hub")

st.title("SF Intelligence Hub")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:

    with st.chat_message(m["role"]):

        st.markdown(m["content"])

        if "chart_data" in m:
            st.bar_chart(pd.DataFrame(m["chart_data"]))

prompt = st.chat_input("Ask about CRM data")

if prompt:

    st.session_state.messages.append({"role":"user","content":prompt})

    response = requests.post(
        f"{API_URL}/chat",
        json={
            "question":prompt,
            "history":st.session_state.messages[:-1]
        }
    )

    data = response.json()

    answer = data["answer"]

    with st.chat_message("assistant"):
        st.markdown(answer)

        if data.get("visual_data"):
            st.bar_chart(pd.DataFrame(data["visual_data"]))

    st.session_state.messages.append({
        "role":"assistant",
        "content":answer,
        "chart_data":data.get("visual_data")
    })