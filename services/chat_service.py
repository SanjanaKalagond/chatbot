from app.llm.orchestrator import process_complex_query

def chat(question, history=[]):
    result = process_complex_query(question, history)
    return result