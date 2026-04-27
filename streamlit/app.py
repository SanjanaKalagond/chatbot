import streamlit as st
import requests
import pandas as pd
import os
API_URL = os.getenv("API_URL","http://localhost:8000")
#API_URL = os.getenv("API_URL","http://fastapi:8000")

st.set_page_config(
    page_title="SF Chatbot",
    page_icon="",
    layout="wide"
)

with st.sidebar:
    st.title("SF Chatbot")
    st.markdown("---")
    st.markdown("**Query genres**")
    st.caption("The planner routes your question to one of these.")
    with st.expander("CRM Database", expanded=False):
        st.markdown(
            "CRM tables: `account`, `contact`, `opportunity`, `orders`, `order_item`, `case_table`."
        )
        st.caption("Show me accounts by industry")
        st.caption("Top 10 opportunities by amount")
        st.caption("Open cases by priority")
        st.caption("List all the purchases in the month of November 2023")
        st.caption("Show me monthly order trends for 2024")
        st.caption("List the orders installed last 3 months")
    with st.expander("B2B Accounts", expanded=False):
        st.caption("How many B2B accounts per billing country?")
        st.caption("Top 15 B2B accounts by annual revenue")
        st.caption("B2B accounts modified in the last 90 days")
        st.caption("B2B accounts with a parent account (hierarchy)")
    with st.expander("Transcripts", expanded=False):
        st.caption("Compare positive vs negative sentiment by month")
        st.caption("List customers with negative sentiment")
        st.caption("Sentiment breakdown by month")
    with st.expander("Documents", expanded=False):
        st.markdown("RAG over ingested Salesforce and uploaded documents.")
    with st.expander("Hybrid", expanded=False):
        st.markdown("Joins **CRM and/or b2b_accounts** with **transcripts** (e.g. sentiment + revenue).")
        st.caption("Industries with the most negative sentiment")
    with st.expander("General", expanded=False):
        st.markdown("Definitions, strategy, Customer Information.")
        st.caption("Customer profile")
        st.caption("Suggest ways to prevent customer dissatisfaction")
    st.markdown("---")
    st.markdown("**Upload a Document**")
    if "uploader_key" not in st.session_state:
        st.session_state.uploader_key = 0
    uploaded_file = st.file_uploader(
        "Upload PDF or Word doc",
        type=["pdf", "docx", "txt"],
        help="Upload a document to query against",
        key=f"doc_uploader_{st.session_state.uploader_key}",
    )
    if uploaded_file:
        with st.spinner("Uploading..."):
            try:
                files = {"file": (uploaded_file.name, uploaded_file.getvalue(), uploaded_file.type)}
                response = requests.post(f"{API_URL}/upload", files=files, timeout=60)
                if response.status_code == 200:
                    st.success(f"Uploaded: {uploaded_file.name}")
                    st.session_state.doc_uploaded = True
                else:
                    st.error("Upload failed.")
            except requests.exceptions.ConnectionError:
                st.error("Cannot connect to backend.")
            except Exception as e:
                st.error(f"Error: {str(e)}")
    if st.button("Clear Chat"):
        st.session_state.messages = []
        st.session_state.doc_uploaded = False
        st.rerun()
    if st.button("Clear Session Documents"):
        try:
            requests.post(f"{API_URL}/clear_session_docs", timeout=10)
            st.session_state.doc_uploaded = False
            st.session_state.uploader_key += 1  
            st.success("Session document cleared. New answers will not use the uploaded file.")
            st.rerun()
        except Exception:
            st.error("Could not clear session documents.")

st.title("Ask your queries")

if "messages" not in st.session_state:
    st.session_state.messages = []
if "doc_uploaded" not in st.session_state:
    st.session_state.doc_uploaded = False
if "last_question" not in st.session_state:
    st.session_state.last_question = ""
if "last_answer" not in st.session_state:
    st.session_state.last_answer = ""

def render_chart(rows):
    try:
        df = pd.DataFrame(rows)
        df = df.dropna(axis=1, how="all")
        df = df.dropna(subset=[df.columns[0]])
        df = df[df[df.columns[0]] != ""]
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        text_cols = df.select_dtypes(exclude="number").columns.tolist()
        if numeric_cols and len(df) > 1:
            index_col = text_cols[0] if text_cols else df.columns[0]
            is_time_series = any(
                keyword in str(index_col).lower()
                for keyword in ["month", "date", "week", "year", "time"]
            )
            if is_time_series:
                st.line_chart(df.set_index(index_col)[numeric_cols])
            else:
                st.bar_chart(df.set_index(index_col)[numeric_cols])
        with st.expander("View Table"):
            st.dataframe(df, use_container_width=True)
    except Exception:
        pass

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])
        if m.get("chart_data"):
            render_chart(m["chart_data"])

prompt = st.chat_input("Ask your query")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.session_state.last_question = prompt

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                response = requests.post(
                    f"{API_URL}/chat",
                    json={
                        "question": prompt,
                        "history": []
                    },
                    timeout=200
                )
                data = response.json()
                answer = data.get("answer", "No answer returned.")
                visual_data = data.get("visual_data")
                st.session_state.last_answer = answer

                st.markdown(answer)

                if visual_data and isinstance(visual_data, dict):
                    source = visual_data.get("source", "postgres")

                    if source == "customer_360":
                        st.caption("Customer View")
                    elif source == "salesforce_live":
                        st.caption("Data fetched live from Salesforce: syncing to database in background")
                    elif source == "not_found":
                        st.caption("No data found in database or Salesforce")
                    elif source == "hybrid":
                        st.caption("Hybrid query: CRM / B2B + Transcripts")
                    elif source == "b2b_accounts":
                        st.caption("B2B Accounts (Business_Account data in b2b_accounts)")

                    if "rows" in visual_data and visual_data["rows"]:
                        render_chart(visual_data["rows"])

                    if "sql" in visual_data and visual_data["sql"] != "customer_360":
                        with st.expander("View SQL"):
                            st.code(visual_data["sql"], language="sql")

                if st.session_state.doc_uploaded:
                    if st.button("Save this interaction"):
                        try:
                            save_response = requests.post(
                                f"{API_URL}/save_interaction",
                                json={
                                    "question": st.session_state.last_question,
                                    "answer": st.session_state.last_answer
                                },
                                timeout=30
                            )
                            save_data = save_response.json()
                            if save_data.get("status") == "saved":
                                st.success(f"Saved to S3: {save_data.get('folder')}")
                            else:
                                st.error("Save failed.")
                        except Exception as e:
                            st.error(f"Error: {str(e)}")

            except requests.exceptions.ConnectionError:
                answer = "Cannot connect to backend. Make sure FastAPI is running on port 8000."
                visual_data = None
                st.error(answer)

            except Exception as e:
                answer = f"Error: {str(e)}"
                visual_data = None
                st.error(answer)

    st.session_state.messages.append({
        "role": "assistant",
        "content": answer,
        "chart_data": visual_data.get("rows") if visual_data and isinstance(visual_data, dict) else None
    })