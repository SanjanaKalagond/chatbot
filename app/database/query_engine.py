from sqlalchemy import text
from app.database.postgres import engine

def run_sql_query(query):

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()

        return [dict(row._mapping) for row in rows]

    except Exception as e:
        return {"error": str(e)}