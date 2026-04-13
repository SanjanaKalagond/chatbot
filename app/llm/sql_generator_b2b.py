"""PostgreSQL SQL generation for the b2b_accounts table (Business_Account record type only)."""

from app.llm.gemini_client import generate_response

B2B_SCHEMA = """
Table: b2b_accounts

Columns (all queryable; do not use SELECT *):
- id TEXT PRIMARY KEY (Salesforce Account Id, 18-char)
- name TEXT
- account_type TEXT (Salesforce Type field)
- industry TEXT
- annual_revenue TEXT (numeric string; CAST AS NUMERIC for sort/aggregate)
- phone TEXT, fax TEXT, website TEXT
- account_source TEXT
- description TEXT
- number_of_employees TEXT (numeric string; CAST AS NUMERIC when comparing)
- owner_id TEXT (Salesforce User Id owning the account)
- parent_id TEXT (parent Account Id; self-FK to b2b_accounts.id for hierarchy)
- billing_street, billing_city, billing_state, billing_postal_code, billing_country TEXT
- shipping_street, shipping_city, shipping_state, shipping_postal_code, shipping_country TEXT
- record_type_id TEXT, record_type_developer_name TEXT (typically 'Business_Account' for all rows)
- raw JSONB/JSON (full Salesforce payload; avoid selecting raw unless user asks for full JSON)
- created_date TIMESTAMP, last_modified TIMESTAMP

Joins (examples):
- b2b_accounts b LEFT JOIN contact c ON c.account_id = b.id
- b2b_accounts b LEFT JOIN opportunity o ON o.account_id = b.id
- b2b_accounts b LEFT JOIN case_table cs ON cs.account_id = b.id
- b2b_accounts child JOIN b2b_accounts parent ON child.parent_id = parent.id

B2B-SPECIFIC RULES:
- This table is ONLY Business_Account records; do not filter record_type_developer_name unless the question asks for record types explicitly.
- Never use SELECT * — list columns explicitly (exclude raw unless needed).
- annual_revenue and number_of_employees are TEXT: use CAST(column AS NUMERIC) for math, ORDER BY amount, SUM, AVG, comparisons.
- Always NULLS LAST on ORDER BY when sorting nullable numeric columns.
- For "top N by revenue" use ORDER BY CAST(annual_revenue AS NUMERIC) DESC NULLS LAST LIMIT N.
- Date filters on last_modified / created_date: CAST(column AS TIMESTAMP) or column >= CURRENT_TIMESTAMP - INTERVAL '30 days'
- ILIKE for case-insensitive name / city / industry search: b.name ILIKE '%Acme%'
- For hierarchy: parent account name via self-join on parent_id.
- Prefer LIMIT 50 or lower unless user asks for "all"; validate_sql may append LIMIT 50.
- Do not use DELETE/UPDATE/DROP; SELECT only.
"""


def generate_b2b_sql(question: str) -> str:
    prompt = f"""
You are a PostgreSQL expert for B2B Salesforce accounts stored in b2b_accounts.

Schema and rules:
{B2B_SCHEMA}

Generate a single valid PostgreSQL SELECT query to answer the question.
Return ONLY the SQL query, no markdown, no explanation, no backticks.

Question:
{question}
"""
    sql = generate_response(prompt).strip()
    if sql.startswith("```"):
        sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql
