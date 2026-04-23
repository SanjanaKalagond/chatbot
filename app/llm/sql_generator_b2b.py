from app.llm.gemini_client import generate_response

B2B_SCHEMA = """
Table: b2b_accounts

This table contains ONLY Salesforce Account records where RecordType.DeveloperName = 'Business_Account'.
This is a specialized subset of accounts. Do NOT use the generic 'account' table for B2B queries.

Columns (all queryable; do not use SELECT *):
- id TEXT PRIMARY KEY (Salesforce Account Id, 18-char)
- name TEXT (account name)
- account_type TEXT (Salesforce Type field: Customer, Prospect, Partner, etc.)
- industry TEXT (industry classification)
- annual_revenue TEXT (numeric string; CAST AS NUMERIC for sort/aggregate/comparison)
- phone TEXT
- fax TEXT
- website TEXT
- account_source TEXT (how the account was acquired)
- description TEXT (account description/notes)
- number_of_employees TEXT (numeric string; CAST AS NUMERIC when comparing/sorting)
- owner_id TEXT (Salesforce User Id owning the account)
- parent_id TEXT (parent Account Id; self-FK to b2b_accounts.id for hierarchy queries)
- billing_street TEXT
- billing_city TEXT
- billing_state TEXT
- billing_postal_code TEXT
- billing_country TEXT
- shipping_street TEXT
- shipping_city TEXT
- shipping_state TEXT
- shipping_postal_code TEXT
- shipping_country TEXT
- record_type_id TEXT
- record_type_developer_name TEXT (always 'Business_Account' for rows in this table)
- raw JSONB (full Salesforce payload; avoid selecting raw unless user explicitly asks for full JSON)
- created_date TIMESTAMP
- last_modified TIMESTAMP

Common Joins:
- b2b_accounts b LEFT JOIN contact c ON c.account_id = b.id
- b2b_accounts b LEFT JOIN opportunity o ON o.account_id = b.id
- b2b_accounts b LEFT JOIN case_table cs ON cs.account_id = b.id
- b2b_accounts b LEFT JOIN orders ord ON ord.account_id = b.id
- b2b_accounts child JOIN b2b_accounts parent ON child.parent_id = parent.id (for parent-child hierarchy)

B2B-SPECIFIC SQL GENERATION RULES:

1. NEVER use SELECT * — always list columns explicitly (exclude raw column unless specifically requested)

2. Numeric text columns (annual_revenue, number_of_employees):
   - Use CAST(column AS NUMERIC) for sorting, math operations, SUM, AVG, comparisons
   - Examples:
     * ORDER BY CAST(annual_revenue AS NUMERIC) DESC NULLS LAST
     * WHERE CAST(number_of_employees AS NUMERIC) > 100
     * SUM(CAST(annual_revenue AS NUMERIC))

3. Always use NULLS LAST when ordering by nullable numeric columns:
   - ORDER BY CAST(annual_revenue AS NUMERIC) DESC NULLS LAST

4. Date filters on last_modified / created_date:
   - Use: WHERE last_modified >= CURRENT_TIMESTAMP - INTERVAL '30 days'
   - Or: WHERE created_date >= '2024-01-01'::TIMESTAMP

5. Case-insensitive text search:
   - Use ILIKE for name, city, industry, etc.
   - Example: WHERE b.name ILIKE '%Acme%'
   - Example: WHERE b.billing_city ILIKE '%New York%'

6. For "top N by revenue/employees":
   - Use: ORDER BY CAST(annual_revenue AS NUMERIC) DESC NULLS LAST LIMIT N

7. For parent-child hierarchy queries:
   - Self-join: FROM b2b_accounts child JOIN b2b_accounts parent ON child.parent_id = parent.id
   - Select parent.name AS parent_account_name, child.name AS child_account_name

8. Grouping and aggregation:
   - Example: SELECT industry, COUNT(*) as account_count FROM b2b_accounts GROUP BY industry
   - Example: SELECT billing_country, AVG(CAST(annual_revenue AS NUMERIC)) FROM b2b_accounts GROUP BY billing_country

9. LIMIT clause:
   - Default to LIMIT 50 unless user specifies "all" or a specific number
   - For "top N" queries, use LIMIT N

10. Security:
    - ONLY SELECT queries allowed (no DELETE, UPDATE, DROP, TRUNCATE, ALTER)
    - Do not expose raw JSONB column unless explicitly requested

11. Record type filter:
    - NOT NEEDED - this table already contains only Business_Account records
    - Do NOT add WHERE record_type_developer_name = 'Business_Account' (redundant)

EXAMPLES:

Query: "How many B2B accounts per billing country?"
SQL:
SELECT 
    COALESCE(NULLIF(TRIM(billing_country), ''), 'Unknown') AS billing_country,
    COUNT(*) AS account_count
FROM b2b_accounts
GROUP BY COALESCE(NULLIF(TRIM(billing_country), ''), 'Unknown')
ORDER BY account_count DESC NULLS LAST
LIMIT 50

Query: "Top 10 B2B accounts by annual revenue"
SQL:
SELECT 
    id,
    name,
    industry,
    annual_revenue,
    billing_city,
    billing_country
FROM b2b_accounts
WHERE annual_revenue IS NOT NULL AND TRIM(annual_revenue) != ''
ORDER BY CAST(annual_revenue AS NUMERIC) DESC NULLS LAST
LIMIT 10

Query: "B2B accounts in Technology industry"
SQL:
SELECT 
    id,
    name,
    annual_revenue,
    number_of_employees,
    billing_city,
    billing_country,
    phone,
    website
FROM b2b_accounts
WHERE industry ILIKE '%Technology%'
ORDER BY name
LIMIT 50

Query: "B2B accounts with parent accounts"
SQL:
SELECT 
    child.id,
    child.name AS account_name,
    child.industry,
    parent.name AS parent_account_name,
    parent.id AS parent_id
FROM b2b_accounts child
JOIN b2b_accounts parent ON child.parent_id = parent.id
ORDER BY parent.name, child.name
LIMIT 50

Query: "B2B accounts modified in last 30 days"
SQL:
SELECT 
    id,
    name,
    industry,
    billing_city,
    billing_country,
    last_modified
FROM b2b_accounts
WHERE last_modified >= CURRENT_TIMESTAMP - INTERVAL '30 days'
ORDER BY last_modified DESC
LIMIT 50

Query: "Average annual revenue by industry for B2B accounts"
SQL:
SELECT 
    COALESCE(NULLIF(TRIM(industry), ''), 'Unknown') AS industry,
    COUNT(*) AS account_count,
    AVG(CAST(annual_revenue AS NUMERIC)) AS avg_revenue,
    SUM(CAST(annual_revenue AS NUMERIC)) AS total_revenue
FROM b2b_accounts
WHERE annual_revenue IS NOT NULL AND TRIM(annual_revenue) != ''
GROUP BY COALESCE(NULLIF(TRIM(industry), ''), 'Unknown')
ORDER BY total_revenue DESC NULLS LAST
LIMIT 50
"""


def generate_b2b_sql(question: str) -> str:
    prompt = f"""
You are a PostgreSQL expert specializing in B2B Salesforce account data.

Schema and rules:
{B2B_SCHEMA}

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