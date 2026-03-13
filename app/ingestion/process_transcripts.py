from sqlalchemy import insert
from app.database.postgres import engine
from app.database.schema import transcripts
from app.sentiment.sentiment_model import analyze_sentiment

def process_transcript(customer_id, text):
    sentiment = analyze_sentiment(text)

    row = {
        "customer_id": customer_id,
        "text": text,
        "sentiment": sentiment
    }

    with engine.begin() as conn:
        conn.execute(insert(transcripts), [row])