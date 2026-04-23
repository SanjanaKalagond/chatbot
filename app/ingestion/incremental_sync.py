import sys
import time
from datetime import datetime
from app.salesforce.objects import CRM_OBJECTS, TRANSCRIPT_OBJECTS
from app.salesforce.auth import get_salesforce_token
from app.salesforce.bulk_client import run_query_stream
from app.database.postgres import engine
from app.database.schema import (
    account,
    b2b_accounts,
    case_table,
    contact,
    documents,
    opportunity,
    order_item,
    orders,
    salesforce_objects,
    transcripts,
)
from app.database.sync_metadata import get_last_sync, set_last_sync
from app.sentiment.sentiment_model import analyze_sentiment
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text

SYNC_INTERVAL_SECONDS = 1200

def _ensure_orders_wc_order_id_column():
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE orders ADD COLUMN IF NOT EXISTS wc_order_id_c TEXT"))

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
        "Account": "Id, Name, Type, Industry, AnnualRevenue, Phone, Website, BillingCity, BillingCountry, LastModifiedDate",
        "Contact": "Id, FirstName, LastName, Email, Phone, AccountId, LastModifiedDate",
        "Opportunity": "Id, Name, Amount, StageName, CloseDate, AccountId, LastModifiedDate",
        "Case": "Id, Subject, Status, Priority, Description, AccountId, LastModifiedDate",
        "Order": "Id, WC_Order_ID__c, AccountId, EffectiveDate, Status, TotalAmount, LastModifiedDate",
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

def _upsert_typed_rows(object_name, rows):
    if not rows:
        return

    if object_name == "Account":
        out = []
        for r in rows:
            out.append(
                {
                    "id": r.get("Id"),
                    "name": r.get("Name"),
                    "industry": r.get("Industry"),
                    "phone": r.get("Phone"),
                    "billing_city": r.get("BillingCity"),
                    "billing_country": r.get("BillingCountry"),
                    "last_modified": parse_sf_datetime(r.get("LastModifiedDate")),
                }
            )
        tbl = account
        set_cols = {
            "name": "name",
            "industry": "industry",
            "phone": "phone",
            "billing_city": "billing_city",
            "billing_country": "billing_country",
            "last_modified": "last_modified",
        }

    elif object_name == "Contact":
        out = []
        for r in rows:
            out.append(
                {
                    "id": r.get("Id"),
                    "first_name": r.get("FirstName"),
                    "last_name": r.get("LastName"),
                    "email": r.get("Email"),
                    "phone": r.get("Phone"),
                    "account_id": r.get("AccountId"),
                    "last_modified": parse_sf_datetime(r.get("LastModifiedDate")),
                }
            )
        tbl = contact
        set_cols = {
            "first_name": "first_name",
            "last_name": "last_name",
            "email": "email",
            "phone": "phone",
            "account_id": "account_id",
            "last_modified": "last_modified",
        }

    elif object_name == "Opportunity":
        out = []
        for r in rows:
            out.append(
                {
                    "id": r.get("Id"),
                    "name": r.get("Name"),
                    "stage": r.get("StageName"),
                    "amount": r.get("Amount"),
                    "close_date": r.get("CloseDate"),
                    "account_id": r.get("AccountId"),
                    "last_modified": parse_sf_datetime(r.get("LastModifiedDate")),
                }
            )
        tbl = opportunity
        set_cols = {
            "name": "name",
            "stage": "stage",
            "amount": "amount",
            "close_date": "close_date",
            "account_id": "account_id",
            "last_modified": "last_modified",
        }

    elif object_name == "Order":
        out = []
        for r in rows:
            wc_order_id = r.get("WC_Order_ID__c") or r.get("WC_Order_ID_c")
            out.append(
                {
                    "id": r.get("Id"),
                    "wc_order_id_c": wc_order_id,
                    "account_id": r.get("AccountId"),
                    "status": r.get("Status"),
                    "effective_date": r.get("EffectiveDate"),
                    "last_modified": parse_sf_datetime(r.get("LastModifiedDate")),
                }
            )
        tbl = orders
        set_cols = {
            "wc_order_id_c": "wc_order_id_c",
            "account_id": "account_id",
            "status": "status",
            "effective_date": "effective_date",
            "last_modified": "last_modified",
        }

    elif object_name == "OrderItem":
        out = []
        for r in rows:
            out.append(
                {
                    "id": r.get("Id"),
                    "order_id": r.get("OrderId"),
                    "quantity": r.get("Quantity"),
                    "unit_price": r.get("UnitPrice"),
                    "total_price": r.get("TotalPrice"),
                    "last_modified": parse_sf_datetime(r.get("LastModifiedDate")),
                }
            )
        tbl = order_item
        set_cols = {
            "order_id": "order_id",
            "quantity": "quantity",
            "unit_price": "unit_price",
            "total_price": "total_price",
            "last_modified": "last_modified",
        }

    elif object_name == "Case":
        out = []
        for r in rows:
            out.append(
                {
                    "id": r.get("Id"),
                    "subject": r.get("Subject"),
                    "status": r.get("Status"),
                    "priority": r.get("Priority"),
                    "account_id": r.get("AccountId"),
                    "last_modified": parse_sf_datetime(r.get("LastModifiedDate")),
                }
            )
        tbl = case_table
        set_cols = {
            "subject": "subject",
            "status": "status",
            "priority": "priority",
            "account_id": "account_id",
            "last_modified": "last_modified",
        }
    else:
        return

    out = [r for r in out if r.get("id")]
    if not out:
        return

    with engine.begin() as conn:
        stmt = insert(tbl).values(out)
        conn.execute(
            stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={k: getattr(stmt.excluded, v) for k, v in set_cols.items()},
            )
        )

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
            subject = r.get("Subject") or ""
            description = r.get("Description") or ""
            
            text_for_sentiment = description if description else subject
            sentiment = analyze_sentiment(text_for_sentiment) if text_for_sentiment.strip() else "NEUTRAL"
            
            rows.append({
                "id": r.get("Id"),
                "object_type": object_name,
                "subject": subject if subject else None,
                "description": description if description else None,
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

def sync_documents(access_token, instance_url):
    key = "documents_ContentVersion"
    last_sync = get_last_sync(key)

    soql = "SELECT Id, Title, FileExtension, FirstPublishLocationId, LastModifiedDate FROM ContentVersion"
    if last_sync:
        sync_str = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
        soql += f" WHERE LastModifiedDate > {sync_str}"

    print(f"Syncing documents since {last_sync or 'beginning'}...")
    sys.stdout.flush()

    total = 0
    for batch in run_query_stream(instance_url, access_token, soql):
        out = []
        for record in batch:
            doc_id = record.get("Id")
            if not doc_id:
                continue
            ext = record.get("FileExtension")
            s3_path = f"docs/{doc_id}.{ext}" if ext else f"docs/{doc_id}"
            out.append(
                {
                    "id": doc_id,
                    "title": record.get("Title"),
                    "file_extension": ext,
                    "linked_entity_id": record.get("FirstPublishLocationId"),
                    "s3_path": s3_path,
                    "last_modified": parse_sf_datetime(record.get("LastModifiedDate")),
                }
            )

        if out:
            with engine.begin() as conn:
                stmt = insert(documents).values(out)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "title": stmt.excluded.title,
                        "file_extension": stmt.excluded.file_extension,
                        "linked_entity_id": stmt.excluded.linked_entity_id,
                        "s3_path": stmt.excluded.s3_path,
                        "last_modified": stmt.excluded.last_modified,
                    },
                )
                conn.execute(stmt)
            total += len(out)
            print(f"documents: {total} records synced")
            sys.stdout.flush()

    set_last_sync(key, datetime.utcnow())
    print(f"Finished documents: {total} new/updated records")

def sync_b2b_accounts(access_token, instance_url):
    key = "Account_B2B"
    last_sync = get_last_sync(key)

    fields = (
        "Id, Name, Type, Industry, AnnualRevenue, Phone, Fax, Website, AccountSource, Description, "
        "NumberOfEmployees, OwnerId, ParentId, "
        "BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, "
        "ShippingStreet, ShippingCity, ShippingState, ShippingPostalCode, ShippingCountry, "
        "RecordTypeId, RecordType.DeveloperName, CreatedDate, LastModifiedDate"
    )
    soql = (
        f"SELECT {fields} FROM Account "
        "WHERE RecordType.DeveloperName = 'Business_Account'"
    )
    if last_sync:
        sync_str = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
        soql += f" AND LastModifiedDate > {sync_str}"

    print(f"Syncing b2b_accounts since {last_sync or 'beginning'}...")
    sys.stdout.flush()

    total = 0
    for batch in run_query_stream(instance_url, access_token, soql):
        out = []
        for r in batch:
            if not r.get("Id"):
                continue
            rt = r.get("RecordType") if isinstance(r.get("RecordType"), dict) else {}
            ne = r.get("NumberOfEmployees")
            out.append(
                {
                    "id": r.get("Id"),
                    "name": r.get("Name"),
                    "account_type": r.get("Type"),
                    "industry": r.get("Industry"),
                    "annual_revenue": r.get("AnnualRevenue"),
                    "phone": r.get("Phone"),
                    "fax": r.get("Fax"),
                    "website": r.get("Website"),
                    "account_source": r.get("AccountSource"),
                    "description": r.get("Description"),
                    "number_of_employees": str(ne) if ne is not None else None,
                    "owner_id": r.get("OwnerId"),
                    "parent_id": r.get("ParentId"),
                    "billing_street": r.get("BillingStreet"),
                    "billing_city": r.get("BillingCity"),
                    "billing_state": r.get("BillingState"),
                    "billing_postal_code": r.get("BillingPostalCode"),
                    "billing_country": r.get("BillingCountry"),
                    "shipping_street": r.get("ShippingStreet"),
                    "shipping_city": r.get("ShippingCity"),
                    "shipping_state": r.get("ShippingState"),
                    "shipping_postal_code": r.get("ShippingPostalCode"),
                    "shipping_country": r.get("ShippingCountry"),
                    "record_type_id": r.get("RecordTypeId"),
                    "record_type_developer_name": rt.get("DeveloperName"),
                    "raw": r,
                    "created_date": parse_sf_datetime(r.get("CreatedDate")),
                    "last_modified": parse_sf_datetime(r.get("LastModifiedDate")),
                }
            )

        if out:
            with engine.begin() as conn:
                stmt = insert(b2b_accounts).values(out)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={c.name: getattr(stmt.excluded, c.name) for c in b2b_accounts.columns if c.name != "id"},
                )
                conn.execute(stmt)
            total += len(out)
            print(f"b2b_accounts: {total} records synced")
            sys.stdout.flush()

    set_last_sync(key, datetime.utcnow())
    print(f"Finished b2b_accounts: {total} new/updated records")

def run_incremental_sync():
    print(f"Starting incremental sync at {datetime.utcnow()}")
    sys.stdout.flush()
    _ensure_orders_wc_order_id_column()

    try:
        access_token, instance_url = get_salesforce_token()
    except Exception as e:
        print(f"Salesforce auth failed: {str(e)}")
        return

    for obj in CRM_OBJECTS:
        try:
            sync_crm_object(obj, access_token, instance_url)

            last_sync = get_last_sync(obj)
            field_map = {
                "Account": "Id, Name, Industry, Phone, BillingCity, BillingCountry, LastModifiedDate",
                "Contact": "Id, FirstName, LastName, Email, Phone, AccountId, LastModifiedDate",
                "Opportunity": "Id, Name, Amount, StageName, CloseDate, AccountId, LastModifiedDate",
                "Case": "Id, Subject, Status, Priority, AccountId, LastModifiedDate",
                "Order": "Id, WC_Order_ID__c, AccountId, EffectiveDate, Status, LastModifiedDate",
                "OrderItem": "Id, OrderId, Quantity, UnitPrice, TotalPrice, LastModifiedDate",
            }
            fields = field_map.get(obj, "Id, LastModifiedDate")
            soql = f"SELECT {fields} FROM {obj}"
            if last_sync:
                sync_str = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
                soql += f" WHERE LastModifiedDate > {sync_str}"
            changed = []
            for batch in run_query_stream(instance_url, access_token, soql):
                changed.extend(batch)
            _upsert_typed_rows(obj, changed)
        except Exception as e:
            print(f"Error syncing {obj}: {str(e)}")
        sys.stdout.flush()

    try:
        sync_b2b_accounts(access_token, instance_url)
    except Exception as e:
        print(f"Error syncing b2b_accounts: {str(e)}")
    sys.stdout.flush()

    for obj in TRANSCRIPT_OBJECTS:
        try:
            sync_transcript_object(obj, access_token, instance_url)
        except Exception as e:
            print(f"Error syncing transcripts {obj}: {str(e)}")
        sys.stdout.flush()

    try:
        sync_documents(access_token, instance_url)
    except Exception as e:
        print(f"Error syncing documents: {str(e)}")
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