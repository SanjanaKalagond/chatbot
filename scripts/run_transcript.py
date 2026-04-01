import sys
import time
from app.ingestion.transcript_pipeline import ingest_transcripts

def run_transcript_phase():
    transcript_objects = ["Task", "Event"]
    
    print("STARTING TRANSCRIPT INGESTION PHASE")
    print(f"Start Time: {time.strftime('%H:%M:%S')}")
    sys.stdout.flush()

    for obj in transcript_objects:
        try:
            print(f"--- Processing {obj} ---")
            ingest_transcripts(obj)
            print(f"--- Finished {obj} ---")
        except Exception as e:
            print(f"Error ingesting transcripts for {obj}: {str(e)}")
        
        sys.stdout.flush()

    print("TRANSCRIPT PHASE COMPLETE")
    print(f"End Time: {time.strftime('%H:%M:%S')}")

if __name__ == "__main__":
    run_transcript_phase()