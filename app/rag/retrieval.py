import faiss
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")
INDEX_PATH = "data/faiss_index/index.faiss"
META_PATH = "data/faiss_index/meta.pkl"
index = faiss.read_index(INDEX_PATH)
with open(META_PATH, "rb") as f:
    metadata = pickle.load(f)
def get_relevant_context(query, k=3):
    q_emb = model.encode([query]).astype("float32")
    D, I = index.search(q_emb, k)
    context = []
    for idx in I[0]:
        if idx < len(metadata):
            context.append(metadata[idx])
    return context