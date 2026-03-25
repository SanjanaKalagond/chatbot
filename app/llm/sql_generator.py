from app.llm.gemini_client import generate_response

SCHEMA = """
PostgreSQL tables:

account(id TEXT, name TEXT, industry TEXT, phone TEXT, billing_city TEXT, billing_country TEXT, last_modified TIMESTAMP)
contact(id TEXT, first_name TEXT, last_name TEXT, email TEXT, phone TEXT, account_id TEXT, last_modified TIMESTAMP)
opportunity(id TEXT, name TEXT, stage TEXT, amount TEXT, close_date TEXT, account_id TEXT, last_modified TIMESTAMP)
orders(id TEXT, account_id TEXT, status TEXT, effective_date TEXT, last_modified TIMESTAMP)
order_item(id TEXT, order_id TEXT, quantity TEXT, unit_price TEXT, total_price TEXT, last_modified TIMESTAMP)
case_table(id TEXT, subject TEXT, status TEXT, priority TEXT, account_id TEXT, last_modified TIMESTAMP)
transcripts(id TEXT, object_type TEXT, subject TEXT, description TEXT, who_id TEXT, what_id TEXT, customer_id TEXT, sentiment TEXT, last_modified TIMESTAMP)
documents(id TEXT, title TEXT, file_extension TEXT, linked_entity_id TEXT, s3_path TEXT, last_modified TIMESTAMP)

Foreign keys:
contact.account_id -> account.id
opportunity.account_id -> account.id
orders.account_id -> account.id
case_table.account_id -> account.id
order_item.order_id -> orders.id
transcripts.customer_id -> contact.id
documents.linked_entity_id -> account.id

IMPORTANT RULES:
- amount, quantity, unit_price, total_price are stored as TEXT but contain numeric values.
- Always cast them using CAST(column AS NUMERIC) when sorting, comparing, or aggregating.
- Always use NULLS LAST when ordering.
- Never use SELECT * — always name the columns explicitly.
- sentiment values are always uppercase: use 'NEGATIVE', 'POSITIVE', 'NEUTRAL' never 'Negative' or 'Positive'.
- When filtering by amount or revenue, do not hardcode thresholds. Use ORDER BY and LIMIT instead to find top results.
- case_table.status values are: 'Open', 'Closed', 'New', 'Solved', 'Pending Hold'.
- When asked for open cases use: status = 'Open'.
"""

def generate_sql(question):
    prompt = f"""
You are a PostgreSQL expert.

Given this schema:
{SCHEMA}

Generate a single valid PostgreSQL SELECT query to answer the question.
Return ONLY the SQL query, no markdown, no explanation, no backticks.

Question:
{question}
"""
    sql = generate_response(prompt).strip()
    if sql.startswith("```"):
        sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql