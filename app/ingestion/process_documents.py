import pdfplumber
from app.rag.chunking import chunk_text
from app.rag.embeddings import generate_embeddings
from app.rag.vector_store import add_vectors

def process_pdf(file_path, doc_id):
    text = ""

    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text

    chunks = chunk_text(text)

    embeddings = generate_embeddings(chunks)

    add_vectors(doc_id, chunks, embeddings)