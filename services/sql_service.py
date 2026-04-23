import threading
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from app.database.postgres import engine
from app.json_sanitize import sanitize_for_json
from app.llm.sql_generator import generate_sql
from app.llm.sql_generator_b2b import generate_b2b_sql

CRM_TABLES = [
    "account",
    "contact",
    "opportunity",
    "orders",
    "order_item",
    "case_table",
    "salesforce_objects",
    "b2b_accounts",
    "transcripts",
    "documents",
    "sync_metadata",
]

METADATA_OBJECTS_SQL = """
SELECT
    n.nspname AS schema_name,
    c.relname AS object_name,
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized view'
        WHEN 'S' THEN 'sequence'
        WHEN 'f' THEN 'foreign table'
        WHEN 'p' THEN 'partitioned table'
        ELSE c.relkind::text
    END AS object_type
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND c.relkind IN ('r', 'v', 'm', 'S', 'f', 'p')
ORDER BY schema_name, object_type, object_name
"""

def validate_sql(sql):
    forbidden = ["delete", "update", "drop", "truncate", "alter", "insert", "create"]
    lowered = sql.lower()
    for word in forbidden:
        if word in lowered:
            raise Exception(f"Unsafe SQL detected: {word}")
    if "select" not in lowered:
        raise Exception("Only SELECT queries allowed")
    if "limit" not in lowered:
        sql = sql.rstrip(";") + " LIMIT 50"
    return sql

def _coerce_types(df):
    import math
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
        else:
            try:
                converted = pd.to_numeric(df[col], errors="coerce")
                if converted.notna().sum() > 0:
                    df[col] = converted
            except Exception:
                pass
    df = df.where(pd.notnull(df), None)
    return df

def _trigger_background_sync():
    def _sync():
        try:
            from app.ingestion.incremental_sync import run_incremental_sync
            run_incremental_sync()
        except Exception as e:
            print(f"Background sync error: {str(e)}")
    thread = threading.Thread(target=_sync, daemon=True)
    thread.start()

def _rule_based_sql(question):
    q = (question or "").lower()
    
    if "case" in q and "priority" in q and any(token in q for token in ["breakdown", "by", "count", "how many"]):
        return """
        SELECT
            COALESCE(NULLIF(TRIM(priority), ''), 'Unknown') AS priority,
            COUNT(*) AS case_count
        FROM case_table
        GROUP BY COALESCE(NULLIF(TRIM(priority), ''), 'Unknown')
        ORDER BY case_count DESC NULLS LAST
        """

    if "industry" not in q:
        return None

    all_accounts_cte = """
    WITH all_accounts AS (
        SELECT a.id, a.industry
        FROM account a
        UNION ALL
        SELECT b.id, b.industry
        FROM b2b_accounts b
        WHERE NOT EXISTS (
            SELECT 1 FROM account a2 WHERE a2.id = b.id
        )
    )
    """

    if "revenue" in q:
        return all_accounts_cte + """
        SELECT
            COALESCE(NULLIF(TRIM(ac.industry), ''), 'Unknown') AS industry,
            SUM(CAST(o.amount AS NUMERIC)) AS total_revenue
        FROM all_accounts ac
        JOIN opportunity o ON o.account_id = ac.id
        WHERE o.amount IS NOT NULL AND TRIM(o.amount) != ''
        GROUP BY COALESCE(NULLIF(TRIM(ac.industry), ''), 'Unknown')
        ORDER BY total_revenue DESC NULLS LAST
        LIMIT 50
        """

    if any(token in q for token in ["graph", "chart", "categorized", "category", "how many", "count"]):
        return all_accounts_cte + """
        SELECT
            COALESCE(NULLIF(TRIM(industry), ''), 'Unknown') AS industry,
            COUNT(*) AS account_count
        FROM all_accounts
        GROUP BY COALESCE(NULLIF(TRIM(industry), ''), 'Unknown')
        ORDER BY account_count DESC NULLS LAST
        LIMIT 50
        """
    return None

def _is_metadata_objects_query(question):
    q = (question or "").lower()
    has_list_intent = any(token in q for token in ["list", "show", "what are", "display"])
    has_object_intent = any(
        token in q
        for token in [
            "objects",
            "tables",
            "views",
            "schema",
            "database objects",
            "db objects",
            "relations",
        ]
    )
    has_db_context = any(token in q for token in ["crm", "database", "db", "postgres", "postgresql"])
    return has_object_intent and (has_list_intent or has_db_context)

def _execute_postgres_sql(sql):
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _coerce_types(df)

def fetch_sample_rows_per_table(limit=5):
    row_limit = max(1, min(int(limit), 50))
    output_rows = []
    with engine.connect() as conn:
        for table_name in CRM_TABLES:
            result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT {row_limit}"))
            rows = result.fetchall()
            columns = list(result.keys())
            df = pd.DataFrame(rows, columns=columns)
            df = _coerce_types(df)
            output_rows.append(
                {
                    "table": table_name,
                    "count": len(df),
                    "rows": sanitize_for_json(df.to_dict(orient="records")),
                }
            )
    return {
        "sql": f"sample_rows_per_table(limit={row_limit})",
        "rows": output_rows,
        "source": "postgres",
    }

def _fallback_to_salesforce(question):
    try:
        from app.llm.sql_generator import generate_sql as gen
        from app.salesforce.live_fetcher import fetch_live_from_sf
        soql = gen(question)
        object_hint = None
        lower = soql.lower()
        object_map = {
            "account": "Account",
            "contact": "Contact",
            "opportunity": "Opportunity",
            "case_table": "Case",
            "orders": "Order",
            "order_item": "OrderItem"
        }
        for table, sf_object in object_map.items():
            if f"from {table}" in lower:
                object_hint = sf_object
                break
        if not object_hint:
            return None
        from app.salesforce.extractor import extract_object_soql
        sf_soql = extract_object_soql(object_hint)
        records = fetch_live_from_sf(sf_soql)
        if not records:
            return None
        df = pd.DataFrame(records)
        df = df.drop(columns=[c for c in df.columns if c.startswith("attributes")], errors="ignore")
        df = _coerce_types(df)
        _trigger_background_sync()
        return {
            "sql": soql,
            "rows": sanitize_for_json(df.head(50).to_dict(orient="records")),
            "source": "salesforce_live"
        }
    except Exception as e:
        print(f"Salesforce fallback error: {str(e)}")
        return None

def handle_sql_query(question):
    if _is_metadata_objects_query(question):
        df = _execute_postgres_sql(METADATA_OBJECTS_SQL)
        return {
            "sql": METADATA_OBJECTS_SQL.strip(),
            "rows": sanitize_for_json(df.to_dict(orient="records")),
            "source": "postgres",
        }

    sql = validate_sql(generate_sql(question))
    
    try:
        df = _execute_postgres_sql(sql)
    except SQLAlchemyError as e:
        print(f"SQL execution error: {str(e)}")
        fallback_sql = _rule_based_sql(question)
        if fallback_sql:
            sql = validate_sql(fallback_sql)
            try:
                df = _execute_postgres_sql(sql)
            except SQLAlchemyError:
                raise
        else:
            raise

    if df.empty:
        print("No data in PostgreSQL, falling back to Salesforce live fetch...")
        fallback = _fallback_to_salesforce(question)
        if fallback:
            return fallback
        return {
            "sql": sql,
            "rows": [],
            "source": "not_found"
        }

    return {
        "sql": sql,
        "rows": sanitize_for_json(df.to_dict(orient="records")),
        "source": "postgres",
    }


def handle_b2b_accounts_query(question):
    sql = generate_b2b_sql(question)
    sql = validate_sql(sql)

    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            rows = result.fetchall()
            columns = list(result.keys())

        df = pd.DataFrame(rows, columns=columns)
        df = _coerce_types(df)

        return {
            "sql": sql,
            "rows": sanitize_for_json(df.to_dict(orient="records")),
            "source": "b2b_accounts",
        }
    except SQLAlchemyError as e:
        print(f"B2B SQL execution error: {str(e)}")
        raise Exception(f"Failed to execute B2B query: {str(e)}")