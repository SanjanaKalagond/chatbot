import sys
import time
from app.ingestion.document_pipeline import ingest_documents


def run_document_phase():

    document_objects = ["ContentVersion"]

    print("STARTING DOCUMENT INGESTION PHASE")
    print(f"Start Time: {time.strftime('%H:%M:%S')}")
    sys.stdout.flush()

    for obj in document_objects:

        try:
            print(f"--- Processing {obj} ---")

            ingest_documents(obj)

            print(f"--- Finished {obj} ---")

        except Exception as e:
            print(f"Error ingesting documents for {obj}: {str(e)}")

        sys.stdout.flush()

    print("DOCUMENT PHASE COMPLETE")
    print(f"End Time: {time.strftime('%H:%M:%S')}")
    sys.stdout.flush()


if __name__ == "__main__":
    run_document_phase()