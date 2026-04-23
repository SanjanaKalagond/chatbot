"""
One-time rebuild for CRM orders table using Salesforce Order object.

What it does:
1) Drops and recreates `orders` table (using current SQLAlchemy schema).
2) Pulls full Order dataset from Salesforce with WC_Order_ID__c.
3) Inserts all rows into `orders` with wc_order_id_c populated when present.
4) Updates sync_metadata cursor for Order.

This script is intentionally standalone so existing sync flow is untouched.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from app.database.postgres import engine
from app.database.schema import orders
from app.database.sync_metadata import set_last_sync
from app.ingestion.incremental_sync import parse_sf_datetime
from app.salesforce.auth import get_salesforce_token
from app.salesforce.bulk_client import run_query_stream


ORDER_SOQL = (
    "SELECT Id, WC_Order_ID__c, AccountId, EffectiveDate, Status, LastModifiedDate "
    "FROM Order"
)


def reset_orders_table() -> None:
    # Keep this isolated to orders only.
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS orders"))
    orders.create(engine, checkfirst=True)


def rebuild_orders_from_salesforce() -> int:
    access_token, instance_url = get_salesforce_token()
    total = 0

    for batch in run_query_stream(instance_url, access_token, ORDER_SOQL):
        rows = []
        for r in batch:
            sf_id = r.get("Id")
            if not sf_id:
                continue
            wc = r.get("WC_Order_ID__c") or r.get("WC_Order_ID_c")
            rows.append(
                {
                    "id": sf_id,
                    "wc_order_id_c": wc,
                    "account_id": r.get("AccountId"),
                    "status": r.get("Status"),
                    "effective_date": r.get("EffectiveDate"),
                    "last_modified": parse_sf_datetime(r.get("LastModifiedDate")),
                }
            )

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
        print(f"orders rebuilt: {total}")

    set_last_sync("Order", datetime.utcnow())
    return total


if __name__ == "__main__":
    print("Dropping and recreating orders table...")
    reset_orders_table()
    print("Rebuilding orders from Salesforce with WC_Order_ID__c...")
    total_rows = rebuild_orders_from_salesforce()
    print(f"Done. Total rows loaded into orders: {total_rows}")
