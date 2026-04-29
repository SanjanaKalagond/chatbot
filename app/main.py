from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
from app.llm.orchestrator import process_complex_query
from app.json_sanitize import sanitize_for_json
from app.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME
import boto3
import os
import faiss
import numpy as np
from datetime import datetime
from starlette.middleware.sessions import SessionMiddleware
from collections import defaultdict
import time

app = FastAPI()

app.add_middleware(SessionMiddleware, secret_key=os.getenv("SECRET_KEY", "change-this-in-production"))
app.add_middleware(GZipMiddleware, minimum_size=1000)

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
    max_age=3600,
)

rate_limit_store = defaultdict(list)
RATE_LIMIT_WINDOW = 60
MAX_REQUESTS = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "60"))

def rate_limit_check(client_ip: str):
    now = time.time()
    rate_limit_store[client_ip] = [req_time for req_time in rate_limit_store[client_ip] if now - req_time < RATE_LIMIT_WINDOW]
    
    if len(rate_limit_store[client_ip]) >= MAX_REQUESTS:
        return False
    
    rate_limit_store[client_ip].append(now)
    return True

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

ALLOWED_EXTENSIONS = {'.pdf', '.docx', '.txt', '.doc', '.ppt', '.pptx', '.xls', '.xlsx'}
MAX_FILE_SIZE = 10 * 1024 * 1024

@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "default-src 'self'"
    return response

@app.post("/chat")
async def chat(req: ChatRequest, request: Request):
    client_ip = request.client.host
    
    if not rate_limit_check(client_ip):
        raise HTTPException(status_code=429, detail="Too many requests. Please try again later.")
    
    if len(req.question) > 5000:
        raise HTTPException(status_code=400, detail="Question too long")
    
    try:
        result = process_complex_query(req.question, req.history, None, session_index, session_metadata)
        return sanitize_for_json(result)
    except Exception as e:
        return sanitize_for_json({
            "answer": f"An error occurred: {str(e)}",
            "visual_data": None
        })

@app.post("/upload")
async def upload_document(file: UploadFile = File(...), request: Request = None):
    global session_index, session_metadata, last_uploaded_filename, last_uploaded_bytes
    
    client_ip = request.client.host
    if not rate_limit_check(client_ip):
        raise HTTPException(status_code=429, detail="Too many requests")
    
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"File type {file_ext} not allowed")
    
    file_bytes = await file.read()
    
    if len(file_bytes) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File too large. Maximum 10MB")
    
    if len(file_bytes) == 0:
        raise HTTPException(status_code=400, detail="Empty file")
    
    tmp_path = f"/tmp/{os.urandom(16).hex()}_{file.filename}"
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
async def save_interaction(req: SaveInteractionRequest, request: Request = None):
    global last_uploaded_filename, last_uploaded_bytes
    
    client_ip = request.client.host
    if not rate_limit_check(client_ip):
        raise HTTPException(status_code=429, detail="Too many requests")
    
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
async def clear_session_docs(request: Request = None):
    global session_index, session_metadata, last_uploaded_filename, last_uploaded_bytes
    
    client_ip = request.client.host
    if not rate_limit_check(client_ip):
        raise HTTPException(status_code=429, detail="Too many requests")
    
    session_index = None
    session_metadata = []
    last_uploaded_filename = None
    last_uploaded_bytes = None
    return {"status": "cleared"}

@app.get("/health")
async def health():
    return {"status": "healthy"}