import time
from src.bronze.ingestion import ingest_to_bronze
from src.silver.transformations import process_silver
from src.gold.fraud_analysis import process_gold

def run_pipeline():
    print("Initializing Real-Time Fraud Detection Pipeline...")
    
    # Start the streams
    bronze_query = ingest_to_bronze()
    print("Bronze Ingestion Active.")
    
    silver_query = process_silver()
    print(" Silver Transformation Active.")
    
    gold_query = process_gold()
    print(" Gold Analysis Active.")

    # In a real app, we would keep these running forever
    # For this POC, we'll let them run for 2 minutes to show data flowing
    time.sleep(120)
    
    bronze_query.stop()
    silver_query.stop()
    gold_query.stop()
    print("🛑 Pipeline stopped successfully.")

if __name__ == "__main__":
    run_pipeline()
