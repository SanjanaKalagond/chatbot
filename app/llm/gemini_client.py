import os
from dotenv import load_dotenv
from google import genai

load_dotenv()

api_key = os.getenv("TEST_KEY2")
if not api_key:
    raise ValueError("GEMINI_API_KEY not found")

client = genai.Client(api_key=api_key)

def generate_response(prompt):
    response = client.models.generate_content(
        model="gemini-2.5-flash-lite",
        contents=prompt
    )
    return response.text