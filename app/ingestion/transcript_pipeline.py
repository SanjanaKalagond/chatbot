from sqlalchemy.dialects.postgresql import insert
import sys
from app.salesforce.auth import get_salesforce_token
from app.salesforce.bulk_client import run_query_stream
from app.database.postgres import engine
from app.database.schema import transcripts
from app.sentiment.sentiment_model import analyze_sentiment

def ingest_transcripts(object_name):
    # Hardcoded SOQL to keep extractor.py untouched
    soql = f"SELECT Id, Subject, Description, WhoId, WhatId, LastModifiedDate FROM {object_name}"
    
    access_token, instance_url = get_salesforce_token()
    
    print(f"Streaming transcripts from {object_name}...")
    total_processed = 0

    for batch in run_query_stream(instance_url, access_token, soql):
        rows = []
        for r in batch:
            content = r.get("Description") or r.get("Subject") or ""
            sentiment_label = analyze_sentiment(content) if content.strip() else "NEUTRAL"
            
            rows.append({
                "id": r.get("Id"),
                "customer_id": r.get("WhoId") or r.get("WhatId"),
                "text": content,
                "sentiment": sentiment_label
            })
        
        if rows:
            with engine.begin() as conn:
                stmt = insert(transcripts).values(rows)
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=['id'],
                    set_={
                        "text": stmt.excluded.text,
                        "sentiment": stmt.excluded.sentiment
                    }
                )
                conn.execute(upsert_stmt)
            
            total_processed += len(rows)
            print(f"Sentiment Analysis Progress: {total_processed} records analyzed and saved")
            sys.stdout.flush()