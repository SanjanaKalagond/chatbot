#incremental_sync.py

import sys
import time
from datetime import datetime
from app.salesforce.objects import CRM_OBJECTS, TRANSCRIPT_OBJECTS
from app.salesforce.auth import get_salesforce_token
from app.salesforce.bulk_client import run_query_stream
from app.database.postgres import engine
from app.database.schema import salesforce_objects, transcripts
from app.database.sync_metadata import get_last_sync, set_last_sync
from app.sentiment.sentiment_model import analyze_sentiment
from sqlalchemy.dialects.postgresql import insert

SYNC_INTERVAL_SECONDS = 1200

def parse_sf_datetime(dt):
    if not dt:
        return None
    try:
        return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.000+0000")
    except Exception:
        return None

def sync_crm_object(object_name, access_token, instance_url):
    last_sync = get_last_sync(object_name)

    field_map = {
        "Account": "Id, Name, Type, Industry, AnnualRevenue, Phone, Website, BillingCity, LastModifiedDate",
        "Contact": "Id, FirstName, LastName, Email, Phone, AccountId, LastModifiedDate",
        "Opportunity": "Id, Name, Amount, StageName, CloseDate, AccountId, LastModifiedDate",
        "Case": "Id, Subject, Status, Priority, Description, AccountId, LastModifiedDate",
        "Order": "Id, AccountId, EffectiveDate, Status, TotalAmount, LastModifiedDate",
        "OrderItem": "Id, OrderId, Quantity, UnitPrice, TotalPrice, LastModifiedDate",
    }

    fields = field_map.get(object_name, "Id, LastModifiedDate")
    soql = f"SELECT {fields} FROM {object_name}"

    if last_sync:
        sync_str = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
        soql += f" WHERE LastModifiedDate > {sync_str}"

    print(f"Syncing {object_name} since {last_sync or 'beginning'}...")
    sys.stdout.flush()

    total = 0

    for batch in run_query_stream(instance_url, access_token, soql):
        rows = []
        for r in batch:
            if "Id" not in r:
                continue
            rows.append({
                "id": r.get("Id"),
                "object_name": object_name,
                "data": r,
                "last_modified": parse_sf_datetime(r.get("LastModifiedDate"))
            })
        if rows:
            with engine.begin() as conn:
                stmt = insert(salesforce_objects).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "data": stmt.excluded.data,
                        "last_modified": stmt.excluded.last_modified
                    }
                )
                conn.execute(stmt)
            total += len(rows)
            print(f"{object_name}: {total} records synced")
            sys.stdout.flush()

    set_last_sync(object_name, datetime.utcnow())
    print(f"Finished {object_name}: {total} new/updated records")

def sync_transcript_object(object_name, access_token, instance_url):
    last_sync = get_last_sync(f"transcript_{object_name}")

    soql = f"SELECT Id, Subject, Description, WhoId, WhatId, LastModifiedDate FROM {object_name}"

    if last_sync:
        sync_str = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
        soql += f" WHERE LastModifiedDate > {sync_str}"

    print(f"Syncing transcripts from {object_name} since {last_sync or 'beginning'}...")
    sys.stdout.flush()

    total = 0

    for batch in run_query_stream(instance_url, access_token, soql):
        rows = []
        for r in batch:
            subject = r.get("Subject")
            description = r.get("Description")
            text_for_sentiment = description or subject or ""
            sentiment = analyze_sentiment(text_for_sentiment) if text_for_sentiment.strip() else "NEUTRAL"
            rows.append({
                "id": r.get("Id"),
                "object_type": object_name,
                "subject": subject,
                "description": description,
                "who_id": r.get("WhoId"),
                "what_id": r.get("WhatId"),
                "customer_id": r.get("WhoId") or r.get("WhatId"),
                "sentiment": sentiment,
                "last_modified": parse_sf_datetime(r.get("LastModifiedDate"))
            })
        if rows:
            with engine.begin() as conn:
                stmt = insert(transcripts).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "subject": stmt.excluded.subject,
                        "description": stmt.excluded.description,
                        "sentiment": stmt.excluded.sentiment,
                        "last_modified": stmt.excluded.last_modified
                    }
                )
                conn.execute(stmt)
            total += len(rows)
            print(f"{object_name} transcripts: {total} records synced")
            sys.stdout.flush()

    set_last_sync(f"transcript_{object_name}", datetime.utcnow())
    print(f"Finished {object_name} transcripts: {total} new/updated records")

def run_incremental_sync():
    print(f"Starting incremental sync at {datetime.utcnow()}")
    sys.stdout.flush()

    try:
        access_token, instance_url = get_salesforce_token()
    except Exception as e:
        print(f"Salesforce auth failed: {str(e)}")
        return

    for obj in CRM_OBJECTS:
        try:
            sync_crm_object(obj, access_token, instance_url)
        except Exception as e:
            print(f"Error syncing {obj}: {str(e)}")
        sys.stdout.flush()

    for obj in TRANSCRIPT_OBJECTS:
        try:
            sync_transcript_object(obj, access_token, instance_url)
        except Exception as e:
            print(f"Error syncing transcripts {obj}: {str(e)}")
        sys.stdout.flush()

    print(f"Incremental sync complete at {datetime.utcnow()}")

def run_sync_loop():
    print(f"Sync loop started. Running every {SYNC_INTERVAL_SECONDS // 60} minutes.")
    while True:
        try:
            run_incremental_sync()
        except Exception as e:
            print(f"Sync loop error: {str(e)}")
        sys.stdout.flush()
        time.sleep(SYNC_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_sync_loop()