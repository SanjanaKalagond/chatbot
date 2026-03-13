import json
from app.llm.gemini_client import model
from app.database.query_engine import run_sql_query
from app.rag.retrieval import get_relevant_context


def process_complex_query(user_query, history, temp_pdf_context=None):

    planner_prompt = f"""
You are a query planner for a Salesforce CRM AI assistant.

DATABASE STRUCTURE:

All CRM data is stored in ONE table:

salesforce_objects

Columns:
- id
- object_name
- data (JSON)
- last_modified

The column `object_name` tells which Salesforce object the row belongs to.

Examples:

Accounts:
SELECT data->>'Name'
FROM salesforce_objects
WHERE object_name = 'Account'
LIMIT 10;

Contacts:
SELECT data->>'FirstName', data->>'LastName'
FROM salesforce_objects
WHERE object_name = 'Contact'
LIMIT 10;

Cases:
SELECT data->>'Subject', data->>'Status'
FROM salesforce_objects
WHERE object_name = 'Case'
LIMIT 10;

Opportunities:
SELECT data->>'Name', data->>'Amount'
FROM salesforce_objects
WHERE object_name = 'Opportunity'
LIMIT 10;

IMPORTANT:
Never query tables like Account, Accounts, Contact, Case.
They do NOT exist.

Always query:
salesforce_objects

User question:
{user_query}

Return JSON only:

{{
"needs_sql": true/false,
"sql_query": "SQL query if needed",
"needs_rag": true/false,
"rag_query": "keywords for document search",
"visualize": true/false
}}
"""

    plan_resp = model.generate_content(planner_prompt)

    clean_json = plan_resp.text.replace("```json", "").replace("```", "").strip()
    plan = json.loads(clean_json)

    results = {}
    visual_data = None

    if plan.get("needs_sql"):
        sql_data = run_sql_query(plan["sql_query"])
        results["crm_data"] = sql_data

        if plan.get("visualize"):
            visual_data = sql_data

    if plan.get("needs_rag"):
        rag_data = get_relevant_context(plan["rag_query"])
        results["doc_data"] = rag_data

    final_prompt = f"""
You are a Salesforce CRM assistant.

Answer the user's question using the data provided.

Question:
{user_query}

Data:
{json.dumps(results)}
"""

    response = model.generate_content(final_prompt)

    return {
        "answer": response.text,
        "visual_data": visual_data
    }