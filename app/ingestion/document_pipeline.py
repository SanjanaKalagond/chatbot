import requests
import sys
import os
from sqlalchemy.dialects.postgresql import insert
from app.salesforce.extractor import extract_object_soql
from app.salesforce.auth import get_salesforce_token
from app.salesforce.bulk_client import run_query_stream
from app.database.postgres import engine
from app.database.schema import documents
from app.ingestion.document_to_s3 import upload_document


def ingest_documents(object_name):

    soql = extract_object_soql(object_name)
    access_token, instance_url = get_salesforce_token()

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    print(f"Streaming document references from {object_name}...")
    total_files = 0

    for batch in run_query_stream(instance_url, access_token, soql):

        rows = []

        for r in batch:

            doc_id = r.get("Id")
            file_url = r.get("VersionData")

            if not doc_id or not file_url:
                continue

            extension = r.get("FileExtension") or "bin"
            extension = extension.lower()

            full_url = instance_url + file_url

            try:

                response = requests.get(full_url, headers=headers, timeout=60)

                if response.status_code != 200:
                    continue

                local_path = f"/tmp/{doc_id}.{extension}"
                s3_key = f"documents/{doc_id}.{extension}"

                with open(local_path, "wb") as f:
                    f.write(response.content)

                s3_path = upload_document(local_path, s3_key)

                rows.append({
                    "id": doc_id,
                    "customer_id": r.get("FirstPublishLocationId"),
                    "s3_path": s3_path,
                    "doc_type": "salesforce_document"
                })

                os.remove(local_path)

            except Exception as e:
                print(f"Error downloading {doc_id}: {str(e)}")

        if rows:

            with engine.begin() as conn:

                stmt = insert(documents).values(rows)

                stmt = stmt.on_conflict_do_nothing()

                conn.execute(stmt)

            total_files += len(rows)

            print(f"Document Transfer Progress: {total_files} files pushed to S3")

            sys.stdout.flush()