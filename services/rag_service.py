from app.rag.retrieval import search

def retrieve_context(query, k=10):
    results = search(query, k=k)
    context = ""
    for r in results:
        context += r["text"] + "\n"
    return context

def handle_rag_query(query):
    results = search(query, k=10)
    return {
        "chunks": [r["text"] for r in results],
        "context": "\n".join([r["text"] for r in results])
    }