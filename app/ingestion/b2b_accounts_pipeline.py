"""
Ingest Salesforce Account records where RecordType.DeveloperName = 'Business_Account'.

Uses sync_metadata.object_name = 'Account_B2B' for incremental cursor, independent of
the generic Account ingest in salesforce_to_postgres / incremental_sync.

Rows are upserted on Salesforce Id (primary key): re-running ingestion updates existing
rows instead of creating duplicates. Incremental runs only query LastModifiedDate > cursor.
"""
from __future__ import annotations

import sys
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from app.database.postgres import engine
from app.database.schema import b2b_accounts
from app.database.sync_metadata import get_last_sync, set_last_sync
from app.ingestion.incremental_sync import parse_sf_datetime
from app.salesforce.auth import get_salesforce_token
from app.salesforce.bulk_client import run_query_stream

B2B_ACCOUNT_SYNC_KEY = "Account_B2B"
RECORD_TYPE_DEVELOPER_NAME = "Business_Account"

# Standard fields (REST query); custom org fields are not included unless you extend this list.
B2B_ACCOUNT_SOQL_FIELDS = (
    "Id, Name, Type, Industry, AnnualRevenue, Phone, Fax, Website, AccountSource, Description, "
    "NumberOfEmployees, OwnerId, ParentId, "
    "BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, "
    "ShippingStreet, ShippingCity, ShippingState, ShippingPostalCode, ShippingCountry, "
    "RecordTypeId, RecordType.DeveloperName, CreatedDate, LastModifiedDate"
)

_B2B_ALTER_DDL = [
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS fax TEXT",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS account_source TEXT",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS description TEXT",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS number_of_employees TEXT",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS owner_id VARCHAR",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS parent_id VARCHAR",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS billing_street TEXT",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS billing_state TEXT",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS billing_postal_code TEXT",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS shipping_street TEXT",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS shipping_city TEXT",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS shipping_state TEXT",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS shipping_postal_code TEXT",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS shipping_country TEXT",
    "ALTER TABLE b2b_accounts ADD COLUMN IF NOT EXISTS created_date TIMESTAMP WITHOUT TIME ZONE",
]

_B2B_INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS ix_b2b_accounts_owner_id ON b2b_accounts (owner_id)",
    "CREATE INDEX IF NOT EXISTS ix_b2b_accounts_parent_id ON b2b_accounts (parent_id)",
]


def _migrate_b2b_accounts_table() -> None:
    with engine.begin() as conn:
        for stmt in _B2B_ALTER_DDL:
            conn.execute(text(stmt))
        for stmt in _B2B_INDEX_DDL:
            conn.execute(text(stmt))


def reset_b2b_accounts_for_full_reload() -> None:
    """Empty b2b_accounts and clear incremental cursor so the next ingest pulls all B2B accounts."""
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE b2b_accounts"))
        conn.execute(
            text("DELETE FROM sync_metadata WHERE object_name = :k"),
            {"k": B2B_ACCOUNT_SYNC_KEY},
        )


def build_b2b_accounts_soql(*, limit: int | None = None, use_last_sync: bool = True) -> str:
    last_sync = get_last_sync(B2B_ACCOUNT_SYNC_KEY) if use_last_sync else None
    soql = (
        f"SELECT {B2B_ACCOUNT_SOQL_FIELDS} FROM Account "
        f"WHERE RecordType.DeveloperName = '{RECORD_TYPE_DEVELOPER_NAME}'"
    )
    if last_sync:
        sync_str = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
        soql += f" AND LastModifiedDate > {sync_str}"
    if limit is not None:
        soql += f" LIMIT {int(limit)}"
    return soql


def _record_type_payload(row: dict) -> tuple[str | None, str | None]:
    rt = row.get("RecordType")
    if isinstance(rt, dict):
        return row.get("RecordTypeId"), rt.get("DeveloperName")
    return row.get("RecordTypeId"), None


def _row_to_record(r: dict) -> dict:
    rt_id, rt_dev = _record_type_payload(r)
    ne = r.get("NumberOfEmployees")
    return {
        "id": r["Id"],
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
        "record_type_id": rt_id,
        "record_type_developer_name": rt_dev,
        "raw": r,
        "created_date": parse_sf_datetime(r.get("CreatedDate")),
        "last_modified": parse_sf_datetime(r.get("LastModifiedDate")),
    }


def ingest_b2b_accounts(*, limit: int | None = None, full_refresh: bool = False) -> None:
    """
    If limit is set, skips sync_metadata updates so a sample run does not affect full/incremental loads.

    full_refresh: TRUNCATE b2b_accounts and reset Account_B2B sync, then load all rows (no limit).
    """
    if full_refresh and limit is not None:
        raise ValueError("Use --full-refresh without --limit")

    b2b_accounts.create(engine, checkfirst=True)
    _migrate_b2b_accounts_table()

    if full_refresh:
        reset_b2b_accounts_for_full_reload()

    if limit is not None:
        soql = build_b2b_accounts_soql(limit=limit, use_last_sync=False)
    elif full_refresh:
        soql = build_b2b_accounts_soql(limit=None, use_last_sync=False)
    else:
        soql = build_b2b_accounts_soql(limit=None, use_last_sync=True)

    access_token, instance_url = get_salesforce_token()

    print(f"B2B accounts SOQL: {soql}")
    sys.stdout.flush()

    total = 0
    for batch in run_query_stream(instance_url, access_token, soql):
        rows_out = []
        for r in batch:
            if not r.get("Id"):
                continue
            rows_out.append(_row_to_record(r))

        if rows_out:
            with engine.begin() as conn:
                stmt = insert(b2b_accounts).values(rows_out)
                ex = stmt.excluded
                upsert = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={c.key: ex[c.key] for c in b2b_accounts.c if c.key != "id"},
                )
                conn.execute(upsert)
            total += len(rows_out)
            print(f"b2b_accounts: upserted {total} rows so far")
            sys.stdout.flush()

    should_set_sync = limit is None
    if should_set_sync:
        set_last_sync(B2B_ACCOUNT_SYNC_KEY, datetime.utcnow())
    else:
        print("Sample run: sync_metadata not updated (no last_sync cursor change).")
        sys.stdout.flush()
    print(f"Finished B2B account ingest. Total upserted this run: {total}")
    sys.stdout.flush()


if __name__ == "__main__":
    ingest_b2b_accounts()
