import sys
import time
from datetime import datetime
from app.ingestion.salesforce_to_postgres import run_full_ingestion
from app.ingestion.b2b_accounts_pipeline import ingest_b2b_accounts
from app.ingestion.document_pipeline import ingest_documents
from app.ingestion.transcript_pipeline import ingest_transcripts
from app.ingestion.build_faiss_index import build_index
from app.database.postgres import engine
from app.database.schema import orders
from app.database.sync_metadata import set_last_sync
from app.salesforce.auth import get_salesforce_token
from app.salesforce.bulk_client import run_query_stream
from app.ingestion.incremental_sync import parse_sf_datetime
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert


ORDER_SOQL = (
    "SELECT Id, WC_Order_ID__c, AccountId, EffectiveDate, Status, LastModifiedDate "
    "FROM Order"
)


def reset_orders_table():
    print("Resetting orders table...")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS orders"))
    orders.create(engine, checkfirst=True)
    print("Orders table reset complete")


def rebuild_orders_from_salesforce():
    print("Rebuilding orders from Salesforce with WC_Order_ID__c...")
    access_token, instance_url = get_salesforce_token()
    total = 0

    for batch in run_query_stream(instance_url, access_token, ORDER_SOQL):
        rows = []
        for r in batch:
            sf_id = r.get("Id")
            if not sf_id:
                continue
            wc = r.get("WC_Order_ID__c") or r.get("WC_Order_ID_c")
            rows.append({
                "id": sf_id,
                "wc_order_id_c": wc,
                "account_id": r.get("AccountId"),
                "status": r.get("Status"),
                "effective_date": r.get("EffectiveDate"),
                "last_modified": parse_sf_datetime(r.get("LastModifiedDate")),
            })

        if not rows:
            continue

        with engine.begin() as conn:
            stmt = insert(orders).values(rows)
            conn.execute(
                stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "wc_order_id_c": stmt.excluded.wc_order_id_c,
                        "account_id": stmt.excluded.account_id,
                        "status": stmt.excluded.status,
                        "effective_date": stmt.excluded.effective_date,
                        "last_modified": stmt.excluded.last_modified,
                    },
                )
            )
        total += len(rows)
        print(f"Orders rebuilt: {total}")

    set_last_sync("Order", datetime.utcnow())
    print(f"Orders rebuild complete: {total} total rows")
    return total


def main():
    start_time = time.time()
    
    print("=" * 80)
    print("FULL SALESFORCE DATA INGESTION - ALL PHASES")
    print(f"Start Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    sys.stdout.flush()

    try:
        print("\n--- PHASE 1: CRM DATA INGESTION ---")
        print("Ingesting Account, Contact, Opportunity, Case, OrderItem...")
        run_full_ingestion()
        print("✓ CRM Data Phase Complete")
    except Exception as e:
        print(f"✗ Error in CRM phase: {str(e)}")
        sys.stdout.flush()

    try:
        print("\n--- PHASE 2: ORDERS REBUILD (with WC_Order_ID__c) ---")
        reset_orders_table()
        rebuild_orders_from_salesforce()
        print("✓ Orders Rebuild Complete")
    except Exception as e:
        print(f"✗ Error rebuilding orders: {str(e)}")
        sys.stdout.flush()

    try:
        print("\n--- PHASE 3: B2B ACCOUNTS INGESTION ---")
        print("Ingesting B2B accounts (RecordType = Business_Account)...")
        ingest_b2b_accounts(limit=None, full_refresh=True)
        print("✓ B2B Accounts Phase Complete")
    except Exception as e:
        print(f"✗ Error in B2B accounts phase: {str(e)}")
        sys.stdout.flush()

    try:
        print("\n--- PHASE 4: TRANSCRIPTS INGESTION ---")
        transcript_objects = ["Task", "Event"]
        for obj in transcript_objects:
            print(f"Ingesting transcripts from {obj}...")
            ingest_transcripts(obj)
        print("✓ Transcripts Phase Complete")
    except Exception as e:
        print(f"✗ Error in transcripts phase: {str(e)}")
        sys.stdout.flush()

    try:
        print("\n--- PHASE 5: DOCUMENTS INGESTION ---")
        print("Ingesting documents from ContentVersion...")
        ingest_documents("ContentVersion")
        print("✓ Documents Phase Complete")
    except Exception as e:
        print(f"✗ Error in documents phase: {str(e)}")
        sys.stdout.flush()

    try:
        print("\n--- PHASE 6: FAISS INDEX BUILD ---")
        print("Building FAISS index for RAG queries...")
        build_index()
        print("✓ FAISS Index Build Complete")
    except Exception as e:
        print(f"✗ Error building FAISS index: {str(e)}")
        sys.stdout.flush()

    end_time = time.time()
    duration = (end_time - start_time) / 60
    
    print("\n" + "=" * 80)
    print("FULL INGESTION PIPELINE COMPLETE")
    print(f"Total Duration: {duration:.2f} minutes")
    print(f"End Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    sys.stdout.flush()


if __name__ == "__main__":
    main()