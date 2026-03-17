from sqlalchemy import text
from app.database.postgres import engine

def check_counts():
    tables = [
        "account",
        "contact",
        "opportunity",
        "orders",
        "order_item",
        "case_table",
        "transcripts",
        "documents",
        "salesforce_objects",
        "sync_metadata"
    ]

    print("\n=== DATABASE ROW COUNTS ===\n")
    with engine.connect() as conn:
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            print(f"{table:<25} {count:>10} rows")

    print("\n=== TRANSCRIPT BREAKDOWN ===\n")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT object_type, COUNT(*) as count
            FROM transcripts
            GROUP BY object_type
            ORDER BY count DESC
        """))
        for row in result:
            print(f"{row[0]:<25} {row[1]:>10} rows")

    print("\n=== SENTIMENT BREAKDOWN ===\n")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT sentiment, COUNT(*) as count
            FROM transcripts
            GROUP BY sentiment
            ORDER BY count DESC
        """))
        for row in result:
            print(f"{row[0]:<25} {row[1]:>10} rows")

    print("\n=== SYNC METADATA ===\n")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT object_name, last_sync_time
            FROM sync_metadata
            ORDER BY object_name
        """))
        for row in result:
            print(f"{row[0]:<25} last synced: {row[1]}")

    print()

if __name__ == "__main__":
    check_counts()