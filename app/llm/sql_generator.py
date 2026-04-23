from app.llm.gemini_client import generate_response

SCHEMA = """
PostgreSQL tables for CRM data:

IMPORTANT: This schema is for GENERIC CRM queries. For B2B account-specific queries (queries mentioning "B2B accounts", "business accounts", "annual revenue", "parent accounts", "number of employees"), use the b2b_accounts table instead via the B2B routing system.

account(id TEXT, name TEXT, industry TEXT, phone TEXT, billing_city TEXT, billing_country TEXT, last_modified TIMESTAMP)
-- Generic account table. Use for queries that don't specifically need B2B-only fields.

contact(id TEXT, first_name TEXT, last_name TEXT, email TEXT, phone TEXT, account_id TEXT, last_modified TIMESTAMP)
-- Contacts linked to accounts via account_id

opportunity(id TEXT, name TEXT, stage TEXT, amount TEXT, close_date TEXT, account_id TEXT, last_modified TIMESTAMP)
-- Sales opportunities. amount is TEXT, cast to NUMERIC for math.

orders(id TEXT, wc_order_id_c TEXT, account_id TEXT, status TEXT, effective_date TEXT, last_modified TIMESTAMP)
-- Orders. wc_order_id_c is the user-facing order ID. effective_date is TEXT in 'YYYY-MM-DD' format.

order_item(id TEXT, order_id TEXT, quantity TEXT, unit_price TEXT, total_price TEXT, last_modified TIMESTAMP)
-- Line items for orders. quantity, unit_price, total_price are TEXT containing numeric values.

case_table(id TEXT, subject TEXT, status TEXT, priority TEXT, account_id TEXT, last_modified TIMESTAMP)
-- Support cases. status values: 'Open', 'Closed', 'New', 'Solved', 'Pending Hold'

transcripts(id TEXT, object_type TEXT, subject TEXT, description TEXT, who_id TEXT, what_id TEXT, customer_id TEXT, sentiment TEXT, last_modified TIMESTAMP)
-- Customer interaction transcripts with sentiment analysis. sentiment values: 'POSITIVE', 'NEGATIVE', 'NEUTRAL' (uppercase)

documents(id TEXT, title TEXT, file_extension TEXT, linked_entity_id TEXT, s3_path TEXT, last_modified TIMESTAMP)
-- Document metadata

b2b_accounts(id TEXT, name TEXT, account_type TEXT, industry TEXT, annual_revenue TEXT, phone TEXT, fax TEXT, website TEXT, account_source TEXT, description TEXT, number_of_employees TEXT, owner_id TEXT, parent_id TEXT, billing_street TEXT, billing_city TEXT, billing_state TEXT, billing_postal_code TEXT, billing_country TEXT, shipping_street TEXT, shipping_city TEXT, shipping_state TEXT, shipping_postal_code TEXT, shipping_country TEXT, record_type_id TEXT, record_type_developer_name TEXT, last_modified TIMESTAMP, created_date TIMESTAMP)
-- B2B accounts ONLY (Business_Account record type subset). Use ONLY when query explicitly needs B2B-specific fields or mentions B2B/business accounts.

Foreign Keys:
contact.account_id -> account.id OR b2b_accounts.id
opportunity.account_id -> account.id OR b2b_accounts.id
orders.account_id -> account.id OR b2b_accounts.id
case_table.account_id -> account.id OR b2b_accounts.id
order_item.order_id -> orders.id
transcripts.customer_id -> contact.id
documents.linked_entity_id -> account.id
b2b_accounts.parent_id -> b2b_accounts.id

CRITICAL SQL GENERATION RULES:

1. TEXT columns with numeric data (amount, quantity, unit_price, total_price, annual_revenue, number_of_employees):
   - ALWAYS cast using CAST(column AS NUMERIC) when sorting, comparing, or aggregating
   - Example: ORDER BY CAST(amount AS NUMERIC) DESC NULLS LAST
   - Example: WHERE CAST(total_price AS NUMERIC) > 1000
   - Example: SUM(CAST(total_price AS NUMERIC))

2. Always use NULLS LAST when ordering by nullable columns:
   - ORDER BY column_name NULLS LAST
   - ORDER BY CAST(amount AS NUMERIC) DESC NULLS LAST

3. NEVER use SELECT * — always name columns explicitly
   - Good: SELECT id, name, amount FROM opportunity
   - Bad: SELECT * FROM opportunity

4. Sentiment values are UPPERCASE:
   - Use 'POSITIVE', 'NEGATIVE', 'NEUTRAL' (not 'Positive', 'positive')
   - Example: WHERE sentiment = 'NEGATIVE'

5. Case table status values (exact case):
   - 'Open', 'Closed', 'New', 'Solved', 'Pending Hold'

6. Orders status values (exact case):
   - 'Completed', 'Cancelled', 'Refunded', 'Processing', 'Shipped', 'Preview', 'Pending'

7. Order ID handling:
   - For user-facing order identifiers, use wc_order_id_c and alias it as order_id
   - Example: SELECT wc_order_id_c AS order_id, status, effective_date FROM orders
   - Use orders.id only for internal joins with order_item

8. Date handling (effective_date and close_date are TEXT in 'YYYY-MM-DD' format):
   - NEVER use date() function
   - ALWAYS use CAST(column AS DATE) or CAST(column AS TIMESTAMP)
   - For "last N months": WHERE CAST(effective_date AS DATE) >= CURRENT_DATE - INTERVAL 'N months'
   - For "till today" or "total": do NOT add date filter
   - Example: WHERE CAST(effective_date AS DATE) >= CURRENT_DATE - INTERVAL '2 months'

9. Time-series queries (trends, over time period):
   - Use DATE_TRUNC for grouping by month/year
   - Example: SELECT DATE_TRUNC('month', CAST(effective_date AS DATE)) AS month, COUNT(*) as order_count
   - GROUP BY month and aggregate (COUNT, SUM, AVG)
   - Do NOT return individual rows for time-series queries

10. Text search (case-insensitive):
    - Use ILIKE for fuzzy matching
    - Example: WHERE name ILIKE '%Acme%'

11. Handling NULL/empty text:
    - Use COALESCE(NULLIF(TRIM(column), ''), 'Unknown') for grouping
    - Example: SELECT COALESCE(NULLIF(TRIM(industry), ''), 'Unknown') AS industry, COUNT(*)

12. Joins:
    - When joining accounts with other tables, consider both account and b2b_accounts
    - For contact/opportunity/case queries, join to account unless B2B-specific fields needed
    - Example hybrid: WITH all_accounts AS (SELECT id, industry FROM account UNION ALL SELECT id, industry FROM b2b_accounts WHERE NOT EXISTS (SELECT 1 FROM account a WHERE a.id = b2b_accounts.id))

13. LIMIT clause:
    - Default to LIMIT 50 unless user asks for "all" or specific number
    - For "top N" queries, use LIMIT N

14. When to use account vs b2b_accounts:
    - Use 'account' for generic CRM queries involving contacts, opportunities, cases, orders
    - Use 'b2b_accounts' ONLY if query explicitly needs: annual_revenue, number_of_employees, parent_id, owner_id, account_source, fax, description, shipping/billing detail, or mentions "B2B" or "business account"

EXAMPLE QUERIES:

Query: "Top 10 opportunities by amount"
SQL:
SELECT 
    id,
    name,
    stage,
    amount,
    close_date,
    account_id
FROM opportunity
WHERE amount IS NOT NULL AND TRIM(amount) != ''
ORDER BY CAST(amount AS NUMERIC) DESC NULLS LAST
LIMIT 10

Query: "Orders in last 2 months"
SQL:
SELECT 
    wc_order_id_c AS order_id,
    account_id,
    status,
    effective_date
FROM orders
WHERE CAST(effective_date AS DATE) >= CURRENT_DATE - INTERVAL '2 months'
ORDER BY CAST(effective_date AS DATE) DESC
LIMIT 50

Query: "Monthly order trends for last 6 months"
SQL:
SELECT 
    DATE_TRUNC('month', CAST(effective_date AS DATE)) AS month,
    COUNT(*) AS order_count,
    SUM(CAST(total_amount AS NUMERIC)) AS total_revenue
FROM orders
WHERE CAST(effective_date AS DATE) >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY DATE_TRUNC('month', CAST(effective_date AS DATE))
ORDER BY month

Query: "Open cases by priority"
SQL:
SELECT 
    COALESCE(NULLIF(TRIM(priority), ''), 'Unknown') AS priority,
    COUNT(*) AS case_count
FROM case_table
WHERE status = 'Open'
GROUP BY COALESCE(NULLIF(TRIM(priority), ''), 'Unknown')
ORDER BY case_count DESC NULLS LAST

Query: "Contacts with negative sentiment"
SQL:
SELECT 
    c.id,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(t.id) AS negative_interaction_count
FROM contact c
JOIN transcripts t ON t.customer_id = c.id
WHERE t.sentiment = 'NEGATIVE'
GROUP BY c.id, c.first_name, c.last_name, c.email
ORDER BY negative_interaction_count DESC
LIMIT 50

Query: "Total revenue by industry"
SQL:
WITH all_accounts AS (
    SELECT id, industry FROM account
    UNION ALL
    SELECT id, industry FROM b2b_accounts
    WHERE NOT EXISTS (SELECT 1 FROM account a WHERE a.id = b2b_accounts.id)
)
SELECT 
    COALESCE(NULLIF(TRIM(ac.industry), ''), 'Unknown') AS industry,
    SUM(CAST(o.amount AS NUMERIC)) AS total_revenue,
    COUNT(o.id) AS opportunity_count
FROM all_accounts ac
JOIN opportunity o ON o.account_id = ac.id
WHERE o.amount IS NOT NULL AND TRIM(o.amount) != ''
GROUP BY COALESCE(NULLIF(TRIM(ac.industry), ''), 'Unknown')
ORDER BY total_revenue DESC NULLS LAST
LIMIT 50
"""

def generate_sql(question):
    prompt = f"""
You are a PostgreSQL expert for Salesforce CRM data.

Schema and rules:
{SCHEMA}

Generate a single valid PostgreSQL SELECT query to answer the question below.
Follow ALL the rules in the schema documentation above.
Return ONLY the SQL query with no markdown formatting, no explanation, no backticks, no code blocks.

Question:
{question}

SQL:
"""
    sql = generate_response(prompt).strip()
    
    if sql.startswith("```"):
        sql = sql.replace("```sql", "").replace("```", "").strip()
    
    lines = sql.split('\n')
    cleaned_lines = []
    for line in lines:
        if line.strip() and not line.strip().startswith('--'):
            cleaned_lines.append(line)
    sql = '\n'.join(cleaned_lines)
    
    return sql