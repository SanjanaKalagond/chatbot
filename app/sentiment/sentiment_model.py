from transformers import pipeline

_analyzer = None

def analyze_sentiment(text):
    global _analyzer
    if _analyzer is None:
        _analyzer = pipeline(
            "sentiment-analysis", 
            model="distilbert-base-uncased-finetuned-sst-2-english",
            device=-1
        )
    
    if not text:
        return "NEUTRAL"
    
    truncated_text = text[:512]
    result = _analyzer(truncated_text)[0]
    return result["label"]