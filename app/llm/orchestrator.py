import json
from app.json_sanitize import sanitize_for_json
from app.llm.gemini_client import generate_response
from services.sql_service import handle_sql_query, handle_b2b_accounts_query, fetch_sample_rows_per_table
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

def is_b2b_query(query):
    q = query.lower()
    
    b2b_explicit_keywords = [
        "b2b account",
        "b2b_account",
        "business account",
        "business_account",
        "recordtype",
        "record type",
        "developerName",
        "business-to-business"
    ]
    
    for keyword in b2b_explicit_keywords:
        if keyword in q:
            return True
    
    b2b_field_keywords = [
        "annual revenue",
        "annualrevenue",
        "number of employees",
        "numberofemployees",
        "parent account",
        "parent_id",
        "owner_id",
        "account owner",
        "shipping address",
        "shipping_",
        "billing address",
        "billing_"
    ]
    
    has_b2b_field = any(keyword in q for keyword in b2b_field_keywords)
    
    has_account_context = any(word in q for word in ["account", "accounts", "company", "companies", "organization"])
    
    if has_b2b_field and has_account_context:
        return True
    
    generic_crm_keywords = [
        "contact",
        "opportunity",
        "case",
        "order",
        "order_item",
        "transcript",
        "sentiment",
        "document"
    ]
    
    has_other_object = any(keyword in q for keyword in generic_crm_keywords)
    
    if has_other_object and not any(keyword in q for keyword in b2b_explicit_keywords):
        return False
    
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

Profile: {json.dumps(sanitize_for_json(data.get('profile', {})))}
Sentiment Summary: {json.dumps(sanitize_for_json(data.get('sentiment_summary', [])))}
Recent Transcripts: {json.dumps(sanitize_for_json(data.get('transcripts', [])[:5]))}
Purchase History: {json.dumps(sanitize_for_json(data.get('purchases', [])[:5]))}
Open Cases: {json.dumps(sanitize_for_json(data.get('cases', [])[:5]))}
Opportunities: {json.dumps(sanitize_for_json(data.get('opportunities', [])[:5]))}
Relevant Documents: {json.dumps(sanitize_for_json(data.get('documents', [])))}

Give a clear, concise summary that helps the support agent assist this customer right now.
Include sentiment trend, recent interactions, purchase history and any open cases.
"""
        answer = generate_response(summary_prompt)
        return {
            "answer": answer,
            "visual_data": sanitize_for_json({
                "sql": "customer_360",
                "rows": data.get("purchases", []),
                "source": "customer_360",
            }),
        }

    q_lower = (user_query or "").lower()
    if "for each table" in q_lower and any(token in q_lower for token in ["record", "row", "sample"]):
        sample_result = fetch_sample_rows_per_table(limit=5)
        table_summaries = [
            f"{entry['table']}: {entry['count']} rows"
            for entry in sample_result.get("rows", [])
        ]
        return {
            "answer": "Here are sample rows per table (up to 5 each):\n" + "\n".join(table_summaries),
            "visual_data": sanitize_for_json(sample_result),
        }

    metadata_object_tokens = [
        "objects",
        "tables",
        "views",
        "schema",
        "database objects",
        "db objects",
        "relations",
    ]
    metadata_list_tokens = ["list", "show", "what are", "display"]
    if any(token in q_lower for token in metadata_object_tokens) and (
        any(token in q_lower for token in metadata_list_tokens)
        or any(token in q_lower for token in ["crm", "database", "db", "postgres", "postgresql"])
    ):
        crm_result = handle_sql_query(user_query)
        rows = sanitize_for_json(crm_result.get("rows", []))
        answer = f"I found {len(rows)} CRM database objects."
        return {
            "answer": answer,
            "visual_data": sanitize_for_json(crm_result),
        }

    if "sentiment" in q_lower:
        if "month" in q_lower or "by month" in q_lower:
            transcript_data = sanitize_for_json(handle_transcript_query(user_query))
            results = {"transcript_data": transcript_data}
            results_str = json.dumps(results)
            final_prompt = f"""
You are a Salesforce CRM assistant.

Answer the question using the data below.
If transcript data is provided, produce a clear breakdown and call out month-by-month comparisons when present.

Question:
{user_query}

Data:
{results_str}
"""
            response_text = generate_response(final_prompt)
            return {
                "answer": response_text,
                "visual_data": {"sql": "transcripts_sentiment_by_month", "rows": transcript_data, "source": "transcripts"},
            }
        if "breakdown" in q_lower or "overview" in q_lower or "summary" in q_lower or "all interactions" in q_lower or "all customer interactions" in q_lower:
            transcript_data = sanitize_for_json(handle_transcript_query(user_query))
            results = {"transcript_data": transcript_data}
            results_str = json.dumps(results)
            final_prompt = f"""
You are a Salesforce CRM assistant.

Answer the question using the data below.
If transcript data is provided, provide a sentiment breakdown with totals and percentages.

Question:
{user_query}

Data:
{results_str}
"""
            response_text = generate_response(final_prompt)
            return {
                "answer": response_text,
                "visual_data": {"sql": "transcripts_sentiment_breakdown", "rows": transcript_data, "source": "transcripts"},
            }

    planner_prompt = f"""
You are a planner for a Salesforce CRM AI assistant.

The system has these data sources:

1. CRM DATABASE
Tables:
account(id, name, industry, phone, billing_city, billing_country)
contact(id, first_name, last_name, email, phone, account_id)
opportunity(id, name, stage, amount, close_date, account_id)
orders(id, wc_order_id_c, account_id, status, effective_date)
order_item(id, order_id, quantity, unit_price, total_price)
case_table(id, subject, status, priority, account_id)

Use 'crm' when:
- Query involves contacts, opportunities, cases, orders, order items
- Generic account queries without specific B2B context
- Multi-table joins not involving B2B-specific fields

2. B2B ACCOUNTS
Table b2b_accounts ONLY: Salesforce Accounts with RecordType DeveloperName Business_Account.
Columns include: id, name, industry, annual_revenue, billing_*, shipping_*, owner_id, parent_id, record_type_developer_name, number_of_employees, account_source, description, fax, website, last_modified, created_date.

Use 'b2b_accounts' when the query explicitly mentions:
- "B2B account", "business account", "Business_Account", "b2b_account"
- Fields unique to b2b_accounts: annual_revenue, number_of_employees, parent_id, owner_id, account_source, fax, description
- Queries about parent-child account hierarchies
- Queries filtering by account owner
- Queries involving shipping/billing addresses in detail
- Revenue analysis at account level
- Employee count analysis

Examples of b2b_accounts queries:
- "How many B2B accounts per billing country?"
- "List business accounts in Technology industry"
- "Top 10 accounts by annual revenue"
- "Accounts with parent accounts"
- "B2B accounts owned by user X"

3. TRANSCRIPTS
transcripts(id, object_type, subject, description, who_id, what_id, customer_id, sentiment, last_modified)
Joined with contact(id, first_name, last_name) on transcripts.customer_id = contact.id

4. DOCUMENTS
Vector search over uploaded Salesforce documents and attachments.

5. HYBRID
Use when the question requires:
- Joining b2b_accounts with transcripts/sentiment
- Joining CRM tables with transcripts
- Cross-analysis involving sentiment and CRM/B2B data
Examples: "B2B accounts with negative sentiment", "industries with most complaints", "high revenue customers with support issues"

6. GENERAL
Use for:
- General knowledge questions (weather, news, definitions)
- Business strategy suggestions based on CRM context
- Advice or recommendations not requiring data lookup
- Conversational questions

CRITICAL ROUTING RULES:
- If query mentions "B2B", "business account", "annual revenue", "number of employees", "parent account", "account owner", or "recordtype" → use 'b2b_accounts'
- If query only mentions generic "account" without B2B context and involves other objects (contacts, cases, opportunities) → use 'crm'
- If query needs sentiment + account data → use 'hybrid'
- If query is about documents or uploaded files → use 'documents'
- If query is conversational or general knowledge → use 'general'

Return ONLY valid JSON:

{{
  "source": "crm" | "b2b_accounts" | "transcripts" | "documents" | "hybrid" | "general",
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
        if is_b2b_query(user_query):
            plan = {"source": "b2b_accounts", "query": user_query, "visualize": False}
        else:
            plan = {"source": "crm", "query": user_query, "visualize": False}

    if plan.get("source") == "b2b":
        plan["source"] = "b2b_accounts"

    results = {}
    visual_data = None
    source = plan.get("source", "crm")

    try:
        if source == "crm":
            crm_result = handle_sql_query(plan["query"])
            results["crm_data"] = crm_result
            visual_data = crm_result

        elif source == "b2b_accounts":
            b2b_result = handle_b2b_accounts_query(plan["query"])
            results["b2b_accounts_data"] = b2b_result
            visual_data = b2b_result

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
            df = df.where(pd.notnull(df), None)
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

    results_str = json.dumps(sanitize_for_json(results))
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
If B2B accounts data is provided, summarize using only b2b_accounts results (Business_Account subset).
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
        "visual_data": sanitize_for_json(visual_data),
    }