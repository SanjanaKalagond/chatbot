from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import sys

from app.database.postgres import engine
from app.database.schema import salesforce_objects
from app.salesforce.objects import CRM_OBJECTS
from app.salesforce.extractor import extract_object_soql
from app.salesforce.bulk_client import run_query_stream
from app.salesforce.auth import get_salesforce_token  # Added missing import
from app.database.sync_metadata import set_last_sync

def ingest_crm_object(object_name):
    # Get the SOQL string from extractor
    soql = extract_object_soql(object_name) 
    access_token, instance_url = get_salesforce_token()
    
    print(f"Streaming ingestion for: {object_name}")
    sys.stdout.flush()
    
    total_count = 0

    # Stream batches from Salesforce and upsert immediately to keep RAM low
    for batch in run_query_stream(instance_url, access_token, soql):
        rows = []
        for r in batch:
            if "Id" not in r: 
                continue
            rows.append({
                "id": r.get("Id"),
                "object_name": object_name,
                "data": r,
                "last_modified": r.get("LastModifiedDate")
            })
        
        if rows:
            with engine.begin() as conn:
                stmt = insert(salesforce_objects).values(rows)
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=['id'],
                    set_={
                        "data": stmt.excluded.data,
                        "last_modified": stmt.excluded.last_modified
                    }
                )
                conn.execute(upsert_stmt)
            
            total_count += len(rows)
            print(f"RDS Update: {total_count} total records saved for {object_name}")
            sys.stdout.flush()
    
    # Track the sync time for the next incremental run
    set_last_sync(object_name, datetime.utcnow())
    print(f"Finished ingestion for {object_name}. Total: {total_count}")

def run_full_ingestion():
    """
    Main entry point called by scripts/run_full_ingestion.py
    Loops through all CRM objects defined in CRM_OBJECTS.
    """
    print("Initializing CRM ingestion loop...")
    for obj in CRM_OBJECTS:
        try:
            ingest_crm_object(obj)
        except Exception as e:
            print(f"CRITICAL ERROR ingesting {obj}: {str(e)}")
            sys.stdout.flush()