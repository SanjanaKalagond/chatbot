"""
B2B Accounts query domain: types of questions, routing rules, and constraints.

Used by sql_generator_b2b (implementation rules). Keep narrative here aligned with B2B_SCHEMA there.
"""

ROUTING_HINTS = """
Choose b2b_accounts when the user clearly means the ingested Business_Account subset, e.g.:
- Mentions: B2B, B2B accounts, business accounts, Business_Account, record type, Salesforce B2B
- Asks for lists/counts/aggregations that should NOT mix all Account types from generic `account` / salesforce_objects
- Focus on: billing/shipping geography, owner, parent hierarchy, revenue/employees on this subset

Prefer CRM (crm) when:
- Generic "accounts" without B2B context and legacy `account` table is OK
- Cross-object CRM without emphasizing B2B-only subset

Prefer hybrid when:
- Need b2b_accounts JOIN transcripts (sentiment) or complex multi-table + sentiment
"""

QUERY_CATEGORIES = {
    "listing_filtering": [
        "List B2B accounts in [country/state/city]",
        "B2B accounts where name contains [text]",
        "B2B accounts in industry [X]",
    ],
    "aggregations": [
        "How many B2B accounts per billing country?",
        "Count B2B accounts by industry",
        "Average annual revenue by industry (CAST annual_revenue)",
    ],
    "ranking": [
        "Top N B2B accounts by annual revenue",
        "Bottom 10 B2B accounts by number of employees",
    ],
    "time": [
        "B2B accounts modified in the last 30 days",
        "B2B accounts created after [date]",
    ],
    "hierarchy": [
        "B2B accounts that have a parent account",
        "Show parent account name for each B2B child (self-join on parent_id)",
    ],
    "joins": [
        "B2B accounts with at least one contact (join contact on account_id = b2b_accounts.id)",
        "Count open cases per B2B account (join case_table)",
    ],
    "ownership": [
        "B2B accounts for owner [owner_id]",
        "Number of B2B accounts per owner_id",
    ],
}

ENGINEERING_RULES = """
- Table is read-only via generated SELECT; validate_sql blocks mutations.
- Text numeric columns: annual_revenue, number_of_employees → CAST(... AS NUMERIC) for math/sort.
- Use ILIKE for fuzzy name/location search.
- Avoid SELECT raw unless the question asks for full Salesforce JSON.
- LIMIT: enforced by validate_sql (max 50) if query omits LIMIT.
"""
