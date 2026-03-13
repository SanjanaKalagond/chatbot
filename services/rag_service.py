from app.rag.retrieval import search

def retrieve_context(query):
    results = search(query)

    context = ""

    for r in results:
        context += r["text"] + "\n"

    return context