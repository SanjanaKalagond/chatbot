#transcript_service.py
import pandas as pd
from sqlalchemy import text
from app.database.postgres import engine

def _serialize_df(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
    return df.to_dict(orient="records")

def get_transcripts_by_sentiment(sentiment, limit=20):
    sql = """
    SELECT
        t.customer_id,
        c.first_name,
        c.last_name,
        t.subject,
        t.description,
        t.sentiment,
        t.last_modified
    FROM transcripts t
    LEFT JOIN contact c ON t.customer_id = c.id
    WHERE UPPER(t.sentiment) = UPPER(:sentiment)
    ORDER BY t.last_modified DESC
    LIMIT :limit
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"sentiment": sentiment, "limit": limit})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize_df(df)

def get_customer_conversations(customer_id, limit=20):
    sql = """
    SELECT
        t.subject,
        t.description,
        t.sentiment,
        t.object_type,
        t.last_modified
    FROM transcripts t
    WHERE t.customer_id = :cid
    ORDER BY t.last_modified DESC
    LIMIT :limit
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"cid": customer_id, "limit": limit})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize_df(df)

def get_sentiment_summary():
    sql = """
    SELECT
        sentiment,
        COUNT(*) as count
    FROM transcripts
    WHERE sentiment IS NOT NULL
    GROUP BY sentiment
    ORDER BY count DESC
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize_df(df)

def get_customers_with_sentiment_and_revenue(sentiment, limit=20):
    sql = """
    SELECT
        c.first_name,
        c.last_name,
        c.email,
        t.sentiment,
        COUNT(t.id) as interaction_count,
        SUM(CAST(o.amount AS NUMERIC)) as total_revenue
    FROM transcripts t
    LEFT JOIN contact c ON t.customer_id = c.id
    LEFT JOIN opportunity o ON o.account_id = c.account_id
    WHERE UPPER(t.sentiment) = UPPER(:sentiment)
    AND o.amount IS NOT NULL
    GROUP BY c.first_name, c.last_name, c.email, t.sentiment
    ORDER BY total_revenue DESC NULLS LAST
    LIMIT :limit
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"sentiment": sentiment, "limit": limit})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
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

def search_transcripts(keyword, limit=20):
    sql = """
    SELECT
        t.customer_id,
        c.first_name,
        c.last_name,
        t.subject,
        t.description,
        t.sentiment,
        t.last_modified
    FROM transcripts t
    LEFT JOIN contact c ON t.customer_id = c.id
    WHERE
        t.subject ILIKE :kw
        OR t.description ILIKE :kw
    ORDER BY t.last_modified DESC
    LIMIT :limit
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"kw": f"%{keyword}%", "limit": limit})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize_df(df)

def handle_transcript_query(question):
    q = question.lower()

    if "negative" in q and ("revenue" in q or "high value" in q or "opportunity" in q):
        return get_customers_with_sentiment_and_revenue("NEGATIVE")

    if "positive" in q and ("revenue" in q or "high value" in q or "opportunity" in q):
        return get_customers_with_sentiment_and_revenue("POSITIVE")

    if "negative" in q:
        return get_transcripts_by_sentiment("NEGATIVE")

    if "positive" in q:
        return get_transcripts_by_sentiment("POSITIVE")

    if "neutral" in q:
        return get_transcripts_by_sentiment("NEUTRAL")

    if "summary" in q or "breakdown" in q or "overview" in q:
        return get_sentiment_summary()

    for word in ["customer_id:", "customer id:", "for customer"]:
        if word in q:
            parts = q.split(word)
            if len(parts) > 1:
                customer_id = parts[1].strip().split()[0]
                return get_customer_conversations(customer_id)

    if "search" in q or "mention" in q or "about" in q:
        words = [w for w in q.split() if len(w) > 4]
        keyword = words[-1] if words else ""
        if keyword:
            return search_transcripts(keyword)

    return get_sentiment_summary()