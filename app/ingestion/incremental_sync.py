from app.salesforce.objects import CRM_OBJECTS
from app.ingestion.salesforce_to_postgres import ingest_object


def run_incremental_sync():

    for obj in CRM_OBJECTS:

        print("Incremental Sync:", obj)

        ingest_object(obj)