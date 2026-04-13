from app.llm.gemini_client import generate_response

SCHEMA = """
PostgreSQL tables:

b2b_accounts(id TEXT, name TEXT, account_type TEXT, industry TEXT, annual_revenue TEXT, phone TEXT, fax TEXT, website TEXT, account_source TEXT, description TEXT, number_of_employees TEXT, owner_id TEXT, parent_id TEXT, billing_street TEXT, billing_city TEXT, billing_state TEXT, billing_postal_code TEXT, billing_country TEXT, shipping_street TEXT, shipping_city TEXT, shipping_state TEXT, shipping_postal_code TEXT, shipping_country TEXT, record_type_id TEXT, record_type_developer_name TEXT, last_modified TIMESTAMP, created_date TIMESTAMP)
-- b2b_accounts = Salesforce Accounts with Business_Account record type only. Join: contact.account_id = b2b_accounts.id, opportunity.account_id = b2b_accounts.id, case_table.account_id = b2b_accounts.id. Self-join parent: child.parent_id = parent.id

account(id TEXT, name TEXT, industry TEXT, phone TEXT, billing_city TEXT, billing_country TEXT, last_modified TIMESTAMP)
contact(id TEXT, first_name TEXT, last_name TEXT, email TEXT, phone TEXT, account_id TEXT, last_modified TIMESTAMP)
opportunity(id TEXT, name TEXT, stage TEXT, amount TEXT, close_date TEXT, account_id TEXT, last_modified TIMESTAMP)
orders(id TEXT, account_id TEXT, status TEXT, effective_date TEXT, last_modified TIMESTAMP)
order_item(id TEXT, order_id TEXT, quantity TEXT, unit_price TEXT, total_price TEXT, last_modified TIMESTAMP)
case_table(id TEXT, subject TEXT, status TEXT, priority TEXT, account_id TEXT, last_modified TIMESTAMP)
transcripts(id TEXT, object_type TEXT, subject TEXT, description TEXT, who_id TEXT, what_id TEXT, customer_id TEXT, sentiment TEXT, last_modified TIMESTAMP)
documents(id TEXT, title TEXT, file_extension TEXT, linked_entity_id TEXT, s3_path TEXT, last_modified TIMESTAMP)

Foreign keys:
contact.account_id -> account.id OR b2b_accounts.id (same Salesforce Id type)
opportunity.account_id -> account.id OR b2b_accounts.id
orders.account_id -> account.id OR b2b_accounts.id
case_table.account_id -> account.id OR b2b_accounts.id
b2b_accounts.parent_id -> b2b_accounts.id
order_item.order_id -> orders.id
transcripts.customer_id -> contact.id
documents.linked_entity_id -> account.id

IMPORTANT RULES:
- amount, quantity, unit_price, total_price are stored as TEXT but contain numeric values.
- Always cast them using CAST(column AS NUMERIC) when sorting, comparing, or aggregating.
- Always use NULLS LAST when ordering.
- Never use SELECT * — always name the columns explicitly.
- sentiment values are always uppercase: use 'NEGATIVE', 'POSITIVE', 'NEUTRAL'.
- When filtering by amount or revenue, do not hardcode thresholds. Use ORDER BY and LIMIT instead.
- case_table.status values are: 'Open', 'Closed', 'New', 'Solved', 'Pending Hold'.
- orders.status values are: 'Completed', 'Cancelled', 'Refunded', 'Processing', 'Shipped', 'Preview', 'Pending'. Always use exact case.
- effective_date is stored as TEXT in format 'YYYY-MM-DD'. To filter by date use: CAST(effective_date AS DATE) >= CURRENT_DATE - INTERVAL '2 months'.
- When filtering dates on TEXT columns always cast first: CAST(column AS DATE).
- Never use date() function. Always use CAST(column AS DATE) or CAST(column AS TIMESTAMP).
- When asked about "last N months" use: CAST(effective_date AS DATE) >= CURRENT_DATE - INTERVAL 'N months'.
- When asked about "till today" or "total" do not add any date filter.
- When asked for orders or trends over a time period, always GROUP BY month and COUNT(*), do not return individual rows.
- For time-series queries use: DATE_TRUNC('month', CAST(effective_date AS DATE)) AS month, COUNT(*) as count
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