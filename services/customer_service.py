import pandas as pd
from sqlalchemy import text
from app.database.postgres import engine
from concurrent.futures import ThreadPoolExecutor

def _serialize(df):
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
    return df.to_dict(orient="records")

def find_customer(name):
    parts = name.strip().split()
    if len(parts) >= 2:
        sql = """
        SELECT c.id, c.first_name, c.last_name, c.email, c.account_id
        FROM contact c
        LEFT JOIN transcripts t ON t.customer_id = c.id
        WHERE c.first_name ILIKE :first AND c.last_name ILIKE :last
        GROUP BY c.id, c.first_name, c.last_name, c.email, c.account_id
        ORDER BY COUNT(t.id) DESC
        LIMIT 1
        """
        with engine.connect() as conn:
            result = conn.execute(text(sql), {"first": parts[0], "last": parts[-1]})
            row = result.fetchone()
            if row:
                return dict(row._mapping)
    sql = """
    SELECT c.id, c.first_name, c.last_name, c.email, c.account_id
    FROM contact c
    LEFT JOIN transcripts t ON t.customer_id = c.id
    WHERE c.first_name ILIKE :name OR c.last_name ILIKE :name
    GROUP BY c.id, c.first_name, c.last_name, c.email, c.account_id
    ORDER BY COUNT(t.id) DESC
    LIMIT 1
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"name": f"%{parts[0]}%"})
        row = result.fetchone()
        if row:
            return dict(row._mapping)
    return None

def get_crm_profile(contact_id, account_id):
    sql = """
    SELECT
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        a.name as account_name,
        a.industry,
        a.billing_city,
        a.billing_country
    FROM contact c
    LEFT JOIN account a ON c.account_id = a.id
    WHERE c.id = :cid
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"cid": contact_id})
        row = result.fetchone()
        if row:
            return dict(row._mapping)
    
    sql_b2b = """
    SELECT
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        b.name as account_name,
        b.industry,
        b.billing_city,
        b.billing_country
    FROM contact c
    LEFT JOIN b2b_accounts b ON c.account_id = b.id
    WHERE c.id = :cid
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql_b2b), {"cid": contact_id})
        row = result.fetchone()
        if row:
            return dict(row._mapping)
    
    return {}

def get_purchase_history(account_id):
    sql = """
    SELECT
        COALESCE(o.wc_order_id_c, o.id) as order_id,
        o.status,
        o.effective_date,
        COALESCE(SUM(CAST(oi.total_price AS NUMERIC)), 0) as total_price
    FROM orders o
    LEFT JOIN order_item oi ON oi.order_id = o.id
    WHERE o.account_id = :aid
    GROUP BY o.id, o.wc_order_id_c, o.status, o.effective_date
    ORDER BY o.effective_date DESC NULLS LAST
    LIMIT 20
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"aid": account_id})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize(df)

def get_cases(account_id):
    sql = """
    SELECT subject, status, priority, last_modified
    FROM case_table
    WHERE account_id = :aid
    ORDER BY last_modified DESC NULLS LAST
    LIMIT 10
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"aid": account_id})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize(df)

def get_opportunities(account_id):
    sql = """
    SELECT name, stage, amount, close_date
    FROM opportunity
    WHERE account_id = :aid
    ORDER BY CAST(amount AS NUMERIC) DESC NULLS LAST
    LIMIT 10
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"aid": account_id})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize(df)

def get_transcript_history(contact_id):
    sql = """
    SELECT
        subject,
        description,
        sentiment,
        object_type,
        last_modified
    FROM transcripts
    WHERE customer_id = :cid
    ORDER BY last_modified DESC NULLS LAST
    LIMIT 20
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"cid": contact_id})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize(df)

def get_sentiment_summary(contact_id):
    sql = """
    SELECT sentiment, COUNT(*) as count
    FROM transcripts
    WHERE customer_id = :cid
    GROUP BY sentiment
    ORDER BY count DESC
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"cid": contact_id})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize(df)

def get_relevant_documents(customer_name):
    from app.rag.retrieval import search
    results = search(customer_name, k=3)
    return [r["text"] for r in results if r.get("text")]

def get_customer_360(customer_name):
    customer = find_customer(customer_name)
    if not customer:
        return {"error": f"Customer '{customer_name}' not found"}

    contact_id = customer["id"]
    account_id = customer.get("account_id")

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            "profile": executor.submit(get_crm_profile, contact_id, account_id),
            "purchases": executor.submit(get_purchase_history, account_id) if account_id else None,
            "cases": executor.submit(get_cases, account_id) if account_id else None,
            "opportunities": executor.submit(get_opportunities, account_id) if account_id else None,
            "transcripts": executor.submit(get_transcript_history, contact_id),
            "sentiment_summary": executor.submit(get_sentiment_summary, contact_id),
            "documents": executor.submit(get_relevant_documents, customer_name)
        }

        results = {"customer": customer}
        for key, future in futures.items():
            if future is None:
                results[key] = []
                continue
            try:
                results[key] = future.result(timeout=30)
            except Exception as e:
                results[key] = {"error": str(e)}

    return results