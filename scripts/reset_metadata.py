from sqlalchemy import text
from app.database.postgres import engine
from app.database.schema import metadata

def reset_sync_tables():
    with engine.connect() as conn:
        print("Dropping sync_metadata to apply unique constraints...")
        conn.execute(text("DROP TABLE IF EXISTS sync_metadata CASCADE;"))
        conn.commit()
    
    print("Recreating tables...")
    metadata.create_all(engine)
    print("Done. You can now run ingestion.")

if __name__ == "__main__":
    reset_sync_tables()