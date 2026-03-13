from app.sql_agent.crm_queries import list_accounts, list_contacts, recent_cases
from app.rag.retrieval import search

def route_query(question):

    q = question.lower()

    if "account" in q or "customer" in q:
        return {"type": "sql", "data": list_accounts()}

    if "contact" in q:
        return {"type": "sql", "data": list_contacts()}

    if "case" in q or "support" in q:
        return {"type": "sql", "data": recent_cases()}

    return {"type": "rag", "data": search(question)}