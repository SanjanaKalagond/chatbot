from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel
from app.llm.orchestrator import process_complex_query
import shutil
import os

app = FastAPI()

class ChatRequest(BaseModel):
    question: str
    history: list = []

@app.post("/chat")
async def chat(req: ChatRequest):
    result = process_complex_query(req.question, req.history)
    return result

@app.post("/upload")
async def upload_document(file: UploadFile = File(...)):
    tmp_path = f"/tmp/{file.filename}"
    with open(tmp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    try:
        from app.rag.blob_parser import extract_text_from_blob
        from app.rag.chunking import chunk_text
        from app.rag.embeddings import generate_embeddings
        from app.rag.vector_store import add_vectors
        text = extract_text_from_blob(tmp_path)
        if text.strip():
            chunks = chunk_text(text)
            embeddings = generate_embeddings(chunks)
            add_vectors(file.filename, chunks, embeddings)
        return {"status": "ok", "filename": file.filename, "chunks": len(chunks) if text.strip() else 0}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)