import threading
import pandas as pd
from sqlalchemy import text
from app.database.postgres import engine
from app.llm.sql_generator import generate_sql

def validate_sql(sql):
    forbidden = ["delete", "update", "drop", "truncate", "alter"]
    lowered = sql.lower()
    for word in forbidden:
        if word in lowered:
            raise Exception("Unsafe SQL detected")
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
            "rows": df.head(50).to_dict(orient="records"),
            "source": "salesforce_live"
        }
    except Exception as e:
        print(f"Salesforce fallback error: {str(e)}")
        return None

def handle_sql_query(question):
    sql = generate_sql(question)
    sql = validate_sql(sql)

    with engine.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.fetchall()
        columns = list(result.keys())

    df = pd.DataFrame(rows, columns=columns)
    df = _coerce_types(df)

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
        "rows": df.to_dict(orient="records"),
        "source": "postgres"
    }