from app.ingestion.salesforce_to_postgres import run_full_ingestion
from app.ingestion.incremental_sync import run_incremental_sync
from app.ingestion.build_faiss_index import build_index

def full_ingestion():
    run_full_ingestion()

def incremental_ingestion():
    run_incremental_sync()

def build_vector_index():
    build_index()