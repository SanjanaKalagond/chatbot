from app.llm.gemini_client import generate_response

def run_chain(question, context):
    prompt = f"Question: {question}\nContext: {context}\nAnswer:"
    answer = generate_response(prompt)
    return answer