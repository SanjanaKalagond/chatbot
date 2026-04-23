ROUTING_HINTS = """
Choose b2b_accounts when the user clearly means the ingested Business_Account subset, e.g.:
- Mentions: B2B, B2B accounts, business accounts, Business_Account, record type, Salesforce B2B
- Asks for data using B2B-specific fields: annual_revenue, number_of_employees, parent_id, owner_id, account_source, fax, description
- Asks about parent-child account hierarchies
- Asks about account ownership (which user owns accounts)
- Needs detailed shipping/billing address breakdown
- Asks for revenue analysis at account level (not opportunity level)

Prefer CRM (crm) when:
- Generic "accounts" without B2B context and legacy `account` table is sufficient
- Cross-object CRM queries involving contacts, opportunities, cases, orders without B2B-specific fields
- Revenue queries based on opportunities (not account annual revenue)

Prefer hybrid when:
- Need b2b_accounts JOIN transcripts (sentiment analysis on B2B accounts)
- Need b2b_accounts JOIN opportunity/case with sentiment
- Complex multi-table queries combining B2B data with CRM interaction data
"""

QUERY_CATEGORIES = {
    "listing_filtering": [
        "List B2B accounts in [country/state/city]",
        "B2B accounts where name contains [text]",
        "B2B accounts in industry [X]",
        "Show all business accounts",
        "Business accounts in Technology sector",
    ],
    "aggregations": [
        "How many B2B accounts per billing country?",
        "Count B2B accounts by industry",
        "Average annual revenue by industry",
        "Total B2B accounts by state",
        "Sum of employees across all B2B accounts",
    ],
    "ranking": [
        "Top N B2B accounts by annual revenue",
        "Bottom 10 B2B accounts by number of employees",
        "Largest business accounts",
        "Smallest B2B accounts by revenue",
        "Top 20 accounts by employee count",
    ],
    "time": [
        "B2B accounts modified in the last 30 days",
        "B2B accounts created after [date]",
        "Recently updated business accounts",
        "New B2B accounts this month",
    ],
    "hierarchy": [
        "B2B accounts that have a parent account",
        "Show parent account name for each B2B child",
        "Parent-child relationships in B2B accounts",
        "Subsidiary accounts",
        "Corporate hierarchy of business accounts",
    ],
    "joins": [
        "B2B accounts with at least one contact",
        "Count open cases per B2B account",
        "B2B accounts with opportunities",
        "Business accounts with recent orders",
    ],
    "ownership": [
        "B2B accounts for owner [owner_id]",
        "Number of B2B accounts per owner_id",
        "Which user owns the most B2B accounts?",
        "Accounts owned by specific sales rep",
    ],
    "revenue_analysis": [
        "B2B accounts with annual revenue over 1M",
        "Average revenue per B2B account",
        "Total annual revenue of all B2B accounts",
        "Revenue distribution by industry",
    ],
    "geography": [
        "B2B accounts by billing country",
        "Business accounts in California",
        "Accounts with shipping address in Europe",
        "Geographic distribution of B2B accounts",
    ],
}

ENGINEERING_RULES = """
- Table is read-only via generated SELECT; validate_sql blocks mutations.
- Text numeric columns: annual_revenue, number_of_employees → CAST(... AS NUMERIC) for math/sort.
- Use ILIKE for fuzzy name/location search.
- Avoid SELECT raw unless the question asks for full Salesforce JSON.
- LIMIT: enforced by validate_sql (max 50) if query omits LIMIT.
- Always use NULLS LAST when ordering by nullable columns.
- Never use SELECT *.
- Handle NULL/empty values: COALESCE(NULLIF(TRIM(column), ''), 'Unknown')
- For parent-child queries: self-join on parent_id.
- Default LIMIT 50 unless user specifies otherwise.
"""

DIFFERENTIATION_FROM_CRM = """
KEY DIFFERENCES between b2b_accounts and generic account/CRM tables:

b2b_accounts:
- ONLY Business_Account record type (subset of all Salesforce Accounts)
- Contains B2B-specific fields: annual_revenue, number_of_employees, parent_id, owner_id, account_source, fax, description
- Full billing/shipping address breakdown (street, city, state, postal_code, country)
- Designed for enterprise/business customer analysis
- Use when query needs: revenue at account level, employee count, parent-child hierarchy, account ownership, detailed address

account (generic CRM):
- All account types mixed together
- Basic fields only: id, name, industry, phone, billing_city, billing_country
- Use when query is generic account lookup without B2B-specific needs
- Use when joining with contacts/opportunities/cases/orders for general CRM queries

ROUTING DECISION TREE:
1. Does query mention "B2B" or "business account"? → b2b_accounts
2. Does query need annual_revenue, number_of_employees, parent_id, owner_id? → b2b_accounts
3. Does query need detailed shipping/billing addresses? → b2b_accounts
4. Does query ask about account hierarchy (parent-child)? → b2b_accounts
5. Does query ask about account ownership? → b2b_accounts
6. Is it a generic account query with contacts/opportunities/cases? → CRM (account table)
7. Does it need both B2B fields AND sentiment/transcripts? → hybrid
"""