from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from app.llm.orchestrator import process_complex_query
from app.json_sanitize import sanitize_for_json
from app.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME
import boto3
import shutil
import os
import faiss
import numpy as np
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

session_index = None
session_metadata = []
last_uploaded_filename = None
last_uploaded_bytes = None

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

class ChatRequest(BaseModel):
    question: str
    history: list = []

class SaveInteractionRequest(BaseModel):
    question: str
    answer: str

@app.post("/chat")
async def chat(req: ChatRequest):
    try:
        result = process_complex_query(req.question, req.history, None, session_index, session_metadata)
        return sanitize_for_json(result)
    except Exception as e:
        return sanitize_for_json({
            "answer": f"An error occurred: {str(e)}",
            "visual_data": None
        })

@app.post("/upload")
async def upload_document(file: UploadFile = File(...)):
    global session_index, session_metadata, last_uploaded_filename, last_uploaded_bytes
    tmp_path = f"/tmp/{file.filename}"
    file_bytes = await file.read()
    last_uploaded_bytes = file_bytes
    last_uploaded_filename = file.filename
    with open(tmp_path, "wb") as buffer:
        buffer.write(file_bytes)
    try:
        from app.rag.blob_parser import extract_text_from_blob
        from app.rag.chunking import chunk_text
        from app.rag.embeddings import generate_embeddings

        text = extract_text_from_blob(tmp_path)
        chunks = []

        if text.strip():
            chunks = chunk_text(text)
            embeddings = generate_embeddings(chunks)
            vectors = np.array(embeddings).astype("float32")
            dimension = vectors.shape[1]

            if session_index is None:
                session_index = faiss.IndexFlatL2(dimension)
                session_metadata = []

            session_index.add(vectors)
            for chunk in chunks:
                session_metadata.append({
                    "doc_id": file.filename,
                    "text": chunk,
                    "source": "session_upload"
                })

        return {"status": "ok", "filename": file.filename, "chunks": len(chunks)}

    except Exception as e:
        return {"status": "error", "detail": str(e)}

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

@app.post("/save_interaction")
async def save_interaction(req: SaveInteractionRequest):
    global last_uploaded_filename, last_uploaded_bytes
    try:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        folder = f"uploads/{timestamp}_{last_uploaded_filename or 'unknown'}"

        if last_uploaded_bytes and last_uploaded_filename:
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=f"{folder}/{last_uploaded_filename}",
                Body=last_uploaded_bytes
            )

        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"{folder}/query.txt",
            Body=req.question.encode("utf-8")
        )

        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"{folder}/reply.txt",
            Body=req.answer.encode("utf-8")
        )

        return {"status": "saved", "folder": folder}

    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.post("/clear_session_docs")
async def clear_session_docs():
    global session_index, session_metadata, last_uploaded_filename, last_uploaded_bytes
    session_index = None
    session_metadata = []
    last_uploaded_filename = None
    last_uploaded_bytes = None
    return {"status": "cleared"}

@app.get("/health")
async def health():
    return {"status": "healthy"}