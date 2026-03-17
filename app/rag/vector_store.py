import os
import faiss
import pickle
import boto3
import numpy as np
from app.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    FAISS_BUCKET_NAME
)

INDEX_FILE = "/tmp/index.faiss"
META_FILE = "/tmp/meta.pkl"
FALLBACK_INDEX = "data/faiss_index/index.faiss"
FALLBACK_META = "data/faiss_index/meta.pkl"

dimension = 384

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def download_index():
    try:
        s3.download_file(FAISS_BUCKET_NAME, "faiss/index.faiss", INDEX_FILE)
        s3.download_file(FAISS_BUCKET_NAME, "faiss/meta.pkl", META_FILE)
        return True
    except Exception:
        return False

def upload_index():
    try:
        s3.upload_file(INDEX_FILE, FAISS_BUCKET_NAME, "faiss/index.faiss")
        s3.upload_file(META_FILE, FAISS_BUCKET_NAME, "faiss/meta.pkl")
    except Exception:
        pass

def load_index():
    if os.path.exists(INDEX_FILE) and os.path.exists(META_FILE):
        index = faiss.read_index(INDEX_FILE)
        with open(META_FILE, "rb") as f:
            metadata = pickle.load(f)
        return index, metadata
    elif os.path.exists(FALLBACK_INDEX) and os.path.exists(FALLBACK_META):
        index = faiss.read_index(FALLBACK_INDEX)
        with open(FALLBACK_META, "rb") as f:
            metadata = pickle.load(f)
        return index, metadata
    return faiss.IndexFlatL2(dimension), []

def add_vectors(doc_id, chunks, embeddings):
    download_index()
    index, metadata = load_index()
    vectors = np.array(embeddings).astype("float32")
    index.add(vectors)
    for chunk in chunks:
        metadata.append({
            "doc_id": doc_id,
            "text": chunk
        })
    faiss.write_index(index, INDEX_FILE)
    with open(META_FILE, "wb") as f:
        pickle.dump(metadata, f)
    upload_index()