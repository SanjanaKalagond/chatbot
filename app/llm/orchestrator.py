import json
from app.llm.gemini_client import generate_response
from services.sql_service import handle_sql_query
from app.llm.sql_generator import generate_sql
from services.transcript_service import handle_transcript_query
from services.rag_service import handle_rag_query
from services.customer_service import get_customer_360

def extract_customer_name(query):
    q = query.lower()
    triggers = [
        "tell me about",
        "history for",
        "profile of",
        "details for",
        "about customer",
        "for customer"
    ]
    for trigger in triggers:
        if trigger in q:
            after = q.split(trigger)[-1].strip()
            words = after.split()
            name_words = []
            for w in words:
                if w in ["is", "the", "a", "an", "their", "his", "her", "and", "with"]:
                    break
                name_words.append(w)
            if name_words:
                candidate = " ".join(name_words).title()
                if len(candidate.split()) >= 2:
                    return candidate
    return None

def is_customer_360_query(query):
    q = query.lower()
    name = extract_customer_name(query)
    if not name:
        return False
    multi_source_hints = [
        "history", "profile", "tell me about", "everything about",
        "purchases", "calls", "complaining", "issue", "concern", "help", "support"
    ]
    for hint in multi_source_hints:
        if hint in q:
            return True
    return False

def process_complex_query(user_query, history=[], temp_pdf_context=None, session_index=None, session_metadata=None):

    if is_customer_360_query(user_query):
        customer_name = extract_customer_name(user_query)
        data = get_customer_360(customer_name)

        if "error" in data:
            return {
                "answer": f"I could not find a customer named '{customer_name}' in the CRM.",
                "visual_data": None
            }

        summary_prompt = f"""
You are a Salesforce CRM assistant helping a support agent.

The agent asked: {user_query}

Here is everything we know about this customer:

Profile: {json.dumps(data.get('profile', {}))}
Sentiment Summary: {json.dumps(data.get('sentiment_summary', []))}
Recent Transcripts: {json.dumps(data.get('transcripts', [])[:5])}
Purchase History: {json.dumps(data.get('purchases', [])[:5])}
Open Cases: {json.dumps(data.get('cases', [])[:5])}
Opportunities: {json.dumps(data.get('opportunities', [])[:5])}
Relevant Documents: {json.dumps(data.get('documents', []))}

Give a clear, concise summary that helps the support agent assist this customer right now.
Include sentiment trend, recent interactions, purchase history and any open cases.
"""
        answer = generate_response(summary_prompt)
        return {
            "answer": answer,
            "visual_data": {
                "sql": "customer_360",
                "rows": data.get("purchases", []),
                "source": "customer_360"
            }
        }

    planner_prompt = f"""
You are a planner for a Salesforce CRM AI assistant.

The system has these data sources:

1. CRM DATABASE
Tables:
account(id, name, industry, phone, billing_city, billing_country)
contact(id, first_name, last_name, email, phone, account_id)
opportunity(id, name, stage, amount, close_date, account_id)
orders(id, account_id, status, effective_date)
order_item(id, order_id, quantity, unit_price, total_price)
case_table(id, subject, status, priority, account_id)

2. TRANSCRIPTS
transcripts(id, object_type, subject, description, who_id, what_id, customer_id, sentiment, last_modified)
Joined with contact(id, first_name, last_name) on transcripts.customer_id = contact.id

3. DOCUMENTS
Vector search over uploaded Salesforce documents and attachments.

4. HYBRID
Use when the question requires joining CRM tables WITH transcripts.
Examples: customers with negative sentiment and high revenue, industries with most negative sentiment.

5. GENERAL
Use for:
- General knowledge questions (weather, news, definitions)
- Business strategy suggestions based on CRM context
- Advice or recommendations not requiring data lookup
- Conversational questions
- Anything not related to querying the database

Return ONLY valid JSON:

{{
  "source": "crm" | "transcripts" | "documents" | "hybrid" | "general",
  "query": "rewritten query if needed, else original",
  "visualize": true | false
}}

User question:
{user_query}
"""

    raw = generate_response(planner_prompt).replace("```json", "").replace("```", "").strip()

    try:
        plan = json.loads(raw)
    except Exception:
        plan = {"source": "crm", "query": user_query, "visualize": False}

    results = {}
    visual_data = None
    source = plan.get("source", "crm")

    try:
        if source == "crm":
            crm_result = handle_sql_query(plan["query"])
            results["crm_data"] = crm_result
            visual_data = crm_result

        elif source == "transcripts":
            transcript_data = handle_transcript_query(plan["query"])
            results["transcript_data"] = transcript_data

        elif source == "documents":
            if session_index is not None and session_metadata:
                from app.rag.retrieval import get_model
                import numpy as np
                m = get_model()
                q_emb = m.encode([plan["query"]]).astype("float32")
                k = min(10, session_index.ntotal)
                D, I = session_index.search(q_emb, k)
                session_chunks = []
                for idx in I[0]:
                    if idx < len(session_metadata):
                        session_chunks.append(session_metadata[idx]["text"])
                results["doc_data"] = "\n".join(session_chunks)
            else:
                rag_data = handle_rag_query(plan["query"])
                results["doc_data"] = rag_data["context"]

        elif source == "hybrid":
            from services.sql_service import validate_sql
            import pandas as pd
            from sqlalchemy import text
            from app.database.postgres import engine

            hybrid_sql = generate_sql(plan["query"])
            hybrid_sql = validate_sql(hybrid_sql)

            with engine.connect() as conn:
                result = conn.execute(text(hybrid_sql))
                rows = result.fetchall()
                columns = list(result.keys())

            df = pd.DataFrame(rows, columns=columns)

            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].astype(str)
                else:
                    try:
                        converted = pd.to_numeric(df[col], errors="coerce")
                        if converted.notna().sum() > 0:
                            df[col] = converted
                    except Exception:
                        pass

            hybrid_result = {
                "sql": hybrid_sql,
                "rows": df.to_dict(orient="records"),
                "source": "hybrid"
            }

            results["hybrid_data"] = hybrid_result
            visual_data = hybrid_result

        elif source == "general":
            results["general"] = True

    except Exception as e:
        results["error"] = str(e)

    results_str = json.dumps(results)
    if len(results_str) > 30000:
        results_str = results_str[:30000] + "... [truncated]"

    if source == "general":
        final_prompt = f"""
You are a helpful AI assistant with knowledge of Salesforce CRM, business strategy, sales, and general topics.

Answer the following question clearly and helpfully.
If it is a business strategy question, provide actionable insights.
If it is a general knowledge question, answer directly.

Question:
{user_query}
"""
    else:
        final_prompt = f"""
You are a Salesforce CRM assistant.

Answer the question using the data below.
If CRM data is provided, summarize it clearly.
If transcript data is provided, explain customer sentiment.
If hybrid data is provided, combine CRM and transcript insights.
If document context is provided, answer using that information.
If data is empty or contains an error, say you could not find relevant information.

Question:
{user_query}

Data:
{results_str}
"""

    response_text = generate_response(final_prompt)

    return {
        "answer": response_text,
        "visual_data": visual_data
    }