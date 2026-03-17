from sqlalchemy import MetaData, Table, Column, String, JSON, DateTime, Integer, Text

metadata = MetaData()

salesforce_objects = Table(
    "salesforce_objects",
    metadata,
    Column("id", String, primary_key=True),
    Column("object_name", String, index=True),
    Column("data", JSON),
    Column("last_modified", DateTime, index=True)
)

transcripts = Table(
    "transcripts",
    metadata,
    Column("id", String, primary_key=True),
    Column("object_type", String, index=True),
    Column("subject", Text),
    Column("description", Text),
    Column("who_id", String, index=True),
    Column("what_id", String, index=True),
    Column("customer_id", String, index=True),
    Column("sentiment", String),
    Column("last_modified", DateTime, index=True)
)

documents = Table(
    "documents",
    metadata,
    Column("id", String, primary_key=True),
    Column("title", Text),
    Column("file_extension", String),
    Column("linked_entity_id", String, index=True),
    Column("s3_path", String),
    Column("last_modified", DateTime, index=True)
)

sync_metadata = Table(
    "sync_metadata",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("object_name", String, unique=True, index=True),
    Column("last_sync_time", DateTime)
)