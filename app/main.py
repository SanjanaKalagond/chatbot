from fastapi import FastAPI
from pydantic import BaseModel
from app.llm.orchestrator import process_complex_query

app = FastAPI()

class ChatRequest(BaseModel):
    question: str
    history: list = []

@app.post("/chat")
async def chat(req: ChatRequest):

    result = process_complex_query(req.question, req.history)

    return result