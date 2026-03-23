from sqlalchemy.dialects.postgresql import insert
import sys
from datetime import datetime
from app.salesforce.auth import get_salesforce_token
from app.salesforce.bulk_client import run_query_stream
from app.database.postgres import engine
from app.database.schema import transcripts
from app.sentiment.sentiment_model import analyze_sentiment

def parse_sf_datetime(dt):
    if not dt:
        return None
    try:
        return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.000+0000")
    except:
        return None

def ingest_transcripts(object_name):
    soql = f"SELECT Id, Subject, Description, WhoId, WhatId, LastModifiedDate FROM {object_name}"
    access_token, instance_url = get_salesforce_token()
    print(f"Streaming transcripts from {object_name}...")
    total_processed = 0
    for batch in run_query_stream(instance_url, access_token, soql):
        rows = []
        for r in batch:
            subject = r.get("Subject")
            description = r.get("Description")
            text_for_sentiment = description or subject or ""
            sentiment_label = analyze_sentiment(text_for_sentiment) if text_for_sentiment.strip() else "NEUTRAL"
            rows.append({
                "id": r.get("Id"),
                "object_type": object_name,
                "subject": subject,
                "description": description,
                "who_id": r.get("WhoId"),
                "what_id": r.get("WhatId"),
                "customer_id": r.get("WhoId") or r.get("WhatId"),
                "sentiment": sentiment_label,
                "last_modified": parse_sf_datetime(r.get("LastModifiedDate"))
            })
        if rows:
            with engine.begin() as conn:
                stmt = insert(transcripts).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['id'],
                    set_={
                        "subject": stmt.excluded.subject,
                        "description": stmt.excluded.description,
                        "sentiment": stmt.excluded.sentiment,
                        "last_modified": stmt.excluded.last_modified
                    }
                )
                conn.execute(stmt)
            total_processed += len(rows)
            print(f"Sentiment Analysis Progress: {total_processed} records analyzed and saved")
            sys.stdout.flush()