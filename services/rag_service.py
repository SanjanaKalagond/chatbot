from app.rag.retrieval import search

def retrieve_context(query):
    results = search(query)
    context = ""
    for r in results:
        context += r["text"] + "\n"
    return context

def handle_rag_query(query):
    return retrieve_context(query)