import os
import faiss
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer

INDEX_PATH = "/tmp/index.faiss"
META_PATH = "/tmp/meta.pkl"
FALLBACK_INDEX = "data/faiss_index/index.faiss"
FALLBACK_META = "data/faiss_index/meta.pkl"

model = None

def get_model():
    global model
    if model is None:
        model = SentenceTransformer("all-MiniLM-L6-v2")
    return model

def load_index():
    if os.path.exists(INDEX_PATH) and os.path.exists(META_PATH):
        index = faiss.read_index(INDEX_PATH)
        with open(META_PATH, "rb") as f:
            metadata = pickle.load(f)
        return index, metadata
    elif os.path.exists(FALLBACK_INDEX) and os.path.exists(FALLBACK_META):
        index = faiss.read_index(FALLBACK_INDEX)
        with open(FALLBACK_META, "rb") as f:
            metadata = pickle.load(f)
        return index, metadata
    return None, []

def search(query, k=10):
    index, metadata = load_index()
    if index is None:
        return []
    m = get_model()
    q_emb = m.encode([query]).astype("float32")
    total = index.ntotal
    k = min(k, total)
    D, I = index.search(q_emb, k)
    results = []
    for idx in I[0]:
        if idx < len(metadata):
            results.append(metadata[idx])
    return results