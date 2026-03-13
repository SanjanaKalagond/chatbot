import sys
import time
from app.ingestion.salesforce_to_postgres import run_full_ingestion
from app.ingestion.document_pipeline import ingest_documents

def main():
    start_time = time.time()
    
    print("STARTING CRM AND DOCUMENT INGESTION")
    print(f"Start Time: {time.strftime('%H:%M:%S')}")
    sys.stdout.flush()

    try:
        print("Phase 1: CRM Data Ingestion (RDS)")
        run_full_ingestion()
        print("CRM Data Phase Finished")
    except Exception as e:
        print(f"Error in CRM phase: {str(e)}")
    
    sys.stdout.flush()

    try:
        print("Phase 2: Document Ingestion (S3 + RDS Metadata)")
        ingest_documents("ContentVersion")
        print("Document Phase Finished")
    except Exception as e:
        print(f"Error in Document phase: {str(e)}")

    end_time = time.time()
    duration = (end_time - start_time) / 60
    
    print("FULL PIPELINE FINISHED")
    print(f"Total Duration: {duration:.2f} minutes")
    sys.stdout.flush()

if __name__ == "__main__":
    main()