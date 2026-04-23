from sqlalchemy import MetaData, Table, Column, String, JSON, DateTime, Integer, Text, TIMESTAMP

metadata = MetaData()

account = Table(
    "account",
    metadata,
    Column("id", Text, primary_key=True),
    Column("name", Text),
    Column("industry", Text),
    Column("phone", Text),
    Column("billing_city", Text),
    Column("billing_country", Text),
    Column("last_modified", TIMESTAMP)
)

contact = Table(
    "contact",
    metadata,
    Column("id", Text, primary_key=True),
    Column("first_name", Text),
    Column("last_name", Text),
    Column("email", Text),
    Column("phone", Text),
    Column("account_id", Text),
    Column("last_modified", TIMESTAMP)
)

opportunity = Table(
    "opportunity",
    metadata,
    Column("id", Text, primary_key=True),
    Column("name", Text),
    Column("stage", Text),
    Column("amount", Text),
    Column("close_date", Text),
    Column("account_id", Text),
    Column("last_modified", TIMESTAMP)
)

orders = Table(
    "orders",
    metadata,
    Column("id", Text, primary_key=True),
    Column("wc_order_id_c", Text),
    Column("account_id", Text),
    Column("status", Text),
    Column("effective_date", Text),
    Column("last_modified", TIMESTAMP)
)

order_item = Table(
    "order_item",
    metadata,
    Column("id", Text, primary_key=True),
    Column("order_id", Text),
    Column("quantity", Text),
    Column("unit_price", Text),
    Column("total_price", Text),
    Column("last_modified", TIMESTAMP)
)

case_table = Table(
    "case_table",
    metadata,
    Column("id", Text, primary_key=True),
    Column("subject", Text),
    Column("status", Text),
    Column("priority", Text),
    Column("account_id", Text),
    Column("last_modified", TIMESTAMP)
)

salesforce_objects = Table(
    "salesforce_objects",
    metadata,
    Column("id", String, primary_key=True),
    Column("object_name", String, index=True),
    Column("data", JSON),
    Column("last_modified", DateTime, index=True)
)

# B2B Account rows only (Salesforce RecordType.DeveloperName = 'Business_Account').
b2b_accounts = Table(
    "b2b_accounts",
    metadata,
    Column("id", String, primary_key=True),
    Column("name", Text),
    Column("account_type", Text),
    Column("industry", Text),
    Column("annual_revenue", Text),
    Column("phone", Text),
    Column("fax", Text),
    Column("website", Text),
    Column("account_source", Text),
    Column("description", Text),
    Column("number_of_employees", Text),
    Column("owner_id", String, index=True),
    Column("parent_id", String, index=True),
    Column("billing_street", Text),
    Column("billing_city", Text),
    Column("billing_state", Text),
    Column("billing_postal_code", Text),
    Column("billing_country", Text),
    Column("shipping_street", Text),
    Column("shipping_city", Text),
    Column("shipping_state", Text),
    Column("shipping_postal_code", Text),
    Column("shipping_country", Text),
    Column("record_type_id", String, index=True),
    Column("record_type_developer_name", String, index=True),
    Column("raw", JSON),
    Column("created_date", TIMESTAMP),
    Column("last_modified", TIMESTAMP, index=True),
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