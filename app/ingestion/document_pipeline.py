import sys
from app.salesforce.auth import get_salesforce_token
from app.salesforce.bulk_client import run_query_stream
from app.database.postgres import engine
from sqlalchemy.dialects.postgresql import insert
from app.database.schema import documents

def ingest_documents(object_name):

    access_token, instance_url = get_salesforce_token()

    soql = """
    SELECT Id, Title, FileExtension, FirstPublishLocationId, LastModifiedDate
    FROM ContentVersion
    """

    for batch in run_query_stream(instance_url, access_token, soql):

        rows = []

        for record in batch:

            doc_id = record.get("Id")
            title = record.get("Title")
            ext = record.get("FileExtension")
            linked = record.get("FirstPublishLocationId")
            last_modified = record.get("LastModifiedDate")

            s3_path = f"docs/{doc_id}.{ext}" if ext else f"docs/{doc_id}"

            rows.append(
                {
                    "id": doc_id,
                    "title": title,
                    "file_extension": ext,
                    "linked_entity_id": linked,
                    "s3_path": s3_path,
                    "last_modified": last_modified
                }
            )

        if rows:

            with engine.begin() as conn:

                stmt = insert(documents).values(rows)

                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "title": stmt.excluded.title,
                        "file_extension": stmt.excluded.file_extension,
                        "linked_entity_id": stmt.excluded.linked_entity_id,
                        "s3_path": stmt.excluded.s3_path,
                        "last_modified": stmt.excluded.last_modified
                    }
                )

                conn.execute(stmt)

        print(f"Ingested {len(rows)} documents")
        sys.stdout.flush()