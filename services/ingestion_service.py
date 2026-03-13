from app.ingestion.salesforce_to_postgres import run_full_ingestion
from app.ingestion.incremental_sync import incremental_accounts
from app.ingestion.build_faiss_index import build_index_from_folder

def full_ingestion():
    run_full_ingestion()

def incremental_ingestion():
    incremental_accounts()

def build_vector_index(folder):
    build_index_from_folder(folder)