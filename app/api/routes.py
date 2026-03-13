from fastapi import APIRouter
from services.chat_service import chat

router = APIRouter()

@router.post("/chat")
def chat_endpoint(payload: dict):
    question = payload["question"]
    answer = chat(question)
    return {"answer": answer}