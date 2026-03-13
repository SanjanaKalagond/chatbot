from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.database.postgres import engine
from app.database.schema import sync_metadata

def get_last_sync(object_name):
    with engine.connect() as conn:
        stmt = select(sync_metadata.c.last_sync_time).where(sync_metadata.c.object_name == object_name)
        result = conn.execute(stmt).fetchone()
        return result[0] if result else None

def set_last_sync(object_name, timestamp):
    with engine.begin() as conn:
        stmt = insert(sync_metadata).values(
            object_name=object_name, 
            last_sync_time=timestamp
        )
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=['object_name'],
            set_={"last_sync_time": stmt.excluded.last_sync_time}
        )
        conn.execute(upsert_stmt)