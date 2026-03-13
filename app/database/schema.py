from sqlalchemy import MetaData, Table, Column, String, JSON, DateTime, Integer, Text

metadata = MetaData()

salesforce_objects = Table(
    "salesforce_objects",
    metadata,
    Column("id", String, primary_key=True),
    Column("object_name", String),
    Column("data", JSON),
    Column("last_modified", DateTime)
)

transcripts = Table(
    "transcripts",
    metadata,
    Column("id", String, primary_key=True),
    Column("customer_id", String),
    Column("text", Text),
    Column("sentiment", String)
)

documents = Table(
    "documents",
    metadata,
    Column("id", String, primary_key=True),
    Column("customer_id", String),
    Column("s3_path", String),
    Column("doc_type", String),
    Column("created_at", DateTime)
)

sync_metadata = Table(
    "sync_metadata",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("object_name", String, unique=True),
    Column("last_sync_time", DateTime)
)