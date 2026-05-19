import os
import pickle
import faiss
import numpy as np
import boto3
from sentence_transformers import SentenceTransformer

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
FAISS_BUCKET_NAME = os.getenv("FAISS_BUCKET_NAME")

INDEX_FILE = "/tmp/index.faiss"
META_FILE = "/tmp/meta.pkl"
FALLBACK_INDEX = "data/faiss_index/index.faiss"
FALLBACK_META = "data/faiss_index/meta.pkl"

dimension = 384

s3 = boto3.client("s3", region_name=AWS_REGION)

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
    if download_index():
        index = faiss.read_index(INDEX_FILE)
        with open(META_FILE, "rb") as f:
            metadata = pickle.load(f)
        return index, metadata
    else:
        try:
            index = faiss.read_index(FALLBACK_INDEX)
            with open(FALLBACK_META, "rb") as f:
                metadata = pickle.load(f)
            return index, metadata
        except Exception:
            index = faiss.IndexFlatL2(dimension)
            metadata = []
            return index, metadata

def search(query: str, top_k: int = 5):
    index, metadata = load_index()
    model = SentenceTransformer("all-MiniLM-L6-v2")
    query_vec = model.encode([query])
    D, I = index.search(np.array(query_vec).astype("float32"), top_k)
    results = []
    for idx in I[0]:
        if idx < len(metadata):
            results.append(metadata[idx])
    return results