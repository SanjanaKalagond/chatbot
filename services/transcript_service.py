import pandas as pd
from sqlalchemy import text
from app.database.postgres import engine


def _serialize_df(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def _serialize_numeric_df(df):
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
    return df.to_dict(orient="records")


def get_transcripts_by_sentiment(sentiment, limit=20):
    sql = """
    SELECT
        t.id,
        t.customer_id,
        c.first_name,
        c.last_name,
        t.subject,
        t.description,
        t.sentiment,
        t.object_type,
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
        t.id,
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


def get_sentiment_summary_overall():
    sql = """
    WITH base AS (
        SELECT UPPER(sentiment) AS sentiment
        FROM transcripts
        WHERE sentiment IS NOT NULL
    )
    SELECT
        sentiment,
        COUNT(*)::BIGINT AS interactions,
        ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct
    FROM base
    GROUP BY sentiment
    ORDER BY interactions DESC
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize_numeric_df(df)


def get_sentiment_by_month():
    sql = """
    SELECT
        DATE_TRUNC('month', last_modified) AS month,
        SUM(CASE WHEN UPPER(sentiment) = 'POSITIVE' THEN 1 ELSE 0 END)::BIGINT AS positive,
        SUM(CASE WHEN UPPER(sentiment) = 'NEGATIVE' THEN 1 ELSE 0 END)::BIGINT AS negative,
        SUM(CASE WHEN UPPER(sentiment) = 'NEUTRAL' THEN 1 ELSE 0 END)::BIGINT AS neutral,
        COUNT(*)::BIGINT AS total
    FROM transcripts
    WHERE sentiment IS NOT NULL
      AND last_modified IS NOT NULL
    GROUP BY 1
    ORDER BY 1
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize_numeric_df(df)


def get_customers_with_sentiment_and_revenue(sentiment, limit=20):
    sql = """
    SELECT
        c.id,
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
    GROUP BY c.id, c.first_name, c.last_name, c.email, t.sentiment
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
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def get_customers_by_interaction_count(sentiment, min_count=5, limit=20):
    sql = """
    SELECT
        c.id,
        c.first_name,
        c.last_name,
        c.email,
        t.sentiment,
        COUNT(t.id) as interactions
    FROM transcripts t
    LEFT JOIN contact c ON t.customer_id = c.id
    WHERE UPPER(t.sentiment) = UPPER(:sentiment)
    AND t.customer_id IS NOT NULL
    GROUP BY c.id, c.first_name, c.last_name, c.email, t.sentiment
    HAVING COUNT(t.id) > :min_count
    ORDER BY interactions DESC
    LIMIT :limit
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {
            "sentiment": sentiment,
            "min_count": min_count,
            "limit": limit
        })
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize_df(df)


def search_transcripts(keyword, limit=20):
    sql = """
    SELECT
        t.id,
        t.customer_id,
        c.first_name,
        c.last_name,
        t.subject,
        t.description,
        t.sentiment,
        t.object_type,
        t.last_modified
    FROM transcripts t
    LEFT JOIN contact c ON t.customer_id = c.id
    WHERE t.subject ILIKE :kw OR t.description ILIKE :kw
    ORDER BY t.last_modified DESC
    LIMIT :limit
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"kw": f"%{keyword}%", "limit": limit})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize_df(df)


def get_transcript_by_name(name):
    parts = name.strip().split()
    if len(parts) >= 2:
        sql = """
        SELECT
            t.id,
            t.subject,
            t.description,
            t.sentiment,
            t.object_type,
            t.last_modified
        FROM transcripts t
        LEFT JOIN contact c ON t.customer_id = c.id
        WHERE c.first_name ILIKE :first AND c.last_name ILIKE :last
        ORDER BY t.last_modified DESC
        LIMIT 20
        """
        with engine.connect() as conn:
            result = conn.execute(text(sql), {"first": parts[0], "last": parts[-1]})
            rows = result.fetchall()
            columns = list(result.keys())
        df = pd.DataFrame(rows, columns=columns)
        return _serialize_df(df)
    return []


def get_voicemail_transcripts(limit=20):
    sql = """
    SELECT
        t.id,
        t.subject,
        t.description,
        t.sentiment,
        t.object_type,
        c.first_name,
        c.last_name,
        t.last_modified
    FROM transcripts t
    LEFT JOIN contact c ON t.customer_id = c.id
    WHERE t.subject ILIKE '%voicemail%'
       OR t.description ILIKE '%voicemail%'
    ORDER BY t.last_modified DESC
    LIMIT :limit
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"limit": limit})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize_df(df)


def get_common_subjects(limit=20):
    sql = """
    SELECT
        subject,
        COUNT(*) as count
    FROM transcripts
    WHERE subject IS NOT NULL AND subject != ''
    GROUP BY subject
    ORDER BY count DESC
    LIMIT :limit
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"limit": limit})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize_df(df)


def get_sample_transcripts(limit=5):
    sql = """
    SELECT
        t.id,
        t.subject,
        t.description,
        t.sentiment,
        t.object_type,
        c.first_name,
        c.last_name,
        t.last_modified
    FROM transcripts t
    LEFT JOIN contact c ON t.customer_id = c.id
    WHERE t.description IS NOT NULL AND LENGTH(t.description) > 100
    ORDER BY t.last_modified DESC
    LIMIT :limit
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"limit": limit})
        rows = result.fetchall()
        columns = list(result.keys())
    df = pd.DataFrame(rows, columns=columns)
    return _serialize_df(df)


def get_transcripts_for_customer_name(customer_name, limit=20):
    parts = customer_name.strip().split()
    
    if len(parts) >= 2:
        first_name = parts[0]
        last_name = parts[-1]
        
        sql = """
        SELECT
            t.id,
            t.subject,
            t.description,
            t.sentiment,
            t.object_type,
            t.last_modified,
            c.first_name,
            c.last_name
        FROM transcripts t
        LEFT JOIN contact c ON t.customer_id = c.id
        WHERE c.first_name ILIKE :first AND c.last_name ILIKE :last
        ORDER BY t.last_modified DESC
        LIMIT :limit
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(sql), {
                "first": first_name,
                "last": last_name,
                "limit": limit
            })
            rows = result.fetchall()
            columns = list(result.keys())
        
        df = pd.DataFrame(rows, columns=columns)
        return _serialize_df(df)
    
    return []


def handle_transcript_query(question):
    q = question.lower()

    if ("sentiment" in q and "month" in q) or "by month" in q:
        return get_sentiment_by_month()

    if "sentiment" in q and ("breakdown" in q or "overview" in q or "summary" in q or "all customer interactions" in q or "all interactions" in q):
        return get_sentiment_summary_overall()

    if "negative" in q and ("revenue" in q or "high value" in q or "opportunity" in q):
        return get_customers_with_sentiment_and_revenue("NEGATIVE")

    if "positive" in q and ("revenue" in q or "high value" in q or "opportunity" in q):
        return get_customers_with_sentiment_and_revenue("POSITIVE")

    if ("more than" in q or "greater than" in q or "over" in q) and "negative" in q:
        return get_customers_by_interaction_count("NEGATIVE")

    if ("more than" in q or "greater than" in q or "over" in q) and "positive" in q:
        return get_customers_by_interaction_count("POSITIVE")

    if "voicemail" in q:
        return get_voicemail_transcripts()

    if "common subject" in q or "frequent subject" in q or "most common" in q:
        return get_common_subjects()

    if ("list transcripts" in q or "show transcripts" in q or "transcripts of" in q or "transcripts for" in q):
        for trigger in ["of ", "for "]:
            if trigger in q:
                name_part = q.split(trigger)[-1].strip()
                if len(name_part.split()) >= 2:
                    return get_transcripts_for_customer_name(name_part.title())
    
    if "display" in q or "conversation" in q or "transcript of" in q:
        for trigger in ["with ", "for ", "of "]:
            if trigger in q:
                name_part = q.split(trigger)[-1].strip().split("\n")[0].strip()
                name_part = name_part.split(":")[0].strip()
                if len(name_part.split()) >= 2:
                    return get_transcript_by_name(name_part.title())
        if "5 customer" in q or "five customer" in q or "sample" in q or "improvement" in q:
            return get_sample_transcripts(5)

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