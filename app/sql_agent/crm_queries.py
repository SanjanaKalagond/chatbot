from sqlalchemy import text
from app.database.postgres import engine

def list_accounts(limit=10):
    query = """
    SELECT data->>'Name'
    FROM salesforce_objects
    WHERE object_name = 'Account'
    LIMIT :limit
    """
    with engine.connect() as conn:
        rows = conn.execute(text(query), {"limit": limit}).fetchall()
    return [r[0] for r in rows]

def list_contacts(limit=10):
    query = """
    SELECT data->>'Name'
    FROM salesforce_objects
    WHERE object_name = 'Contact'
    LIMIT :limit
    """
    with engine.connect() as conn:
        rows = conn.execute(text(query), {"limit": limit}).fetchall()
    return [r[0] for r in rows]

def recent_cases(limit=10):
    query = """
    SELECT data->>'Subject'
    FROM salesforce_objects
    WHERE object_name = 'Case'
    ORDER BY last_modified DESC
    LIMIT :limit
    """
    with engine.connect() as conn:
        rows = conn.execute(text(query), {"limit": limit}).fetchall()
    return [r[0] for r in rows]