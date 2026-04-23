from pyspark.sql import functions as F
from src.utils.config import get_spark_session, PIPELINE_CONFIG

def upsert_to_gold(micro_batch_df, batch_id):
    """
    Handles Idempotency using Delta Lake MERGE.
    Ensures that re-processing a batch doesn't create duplicate fraud records.
    """
    # Create a temporary view for the micro-batch
    micro_batch_df.createOrReplaceTempView("batch_updates")
    
    # Standard Senior DE practice: Use MERGE INTO to prevent duplicates
    spark = micro_batch_df._jdf.sparkSession()
    spark.sql(f"""
        MERGE INTO delta.`{PIPELINE_CONFIG['gold_path']}` AS target
        USING batch_updates AS source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED THEN 
            UPDATE SET target.fraud_score = source.fraud_score, target.is_fraud = source.is_fraud
        WHEN NOT MATCHED THEN 
            INSERT *
    """)

def process_gold():
    spark = get_spark_session()
    
    # Static Merchant Metadata (Join optimized via Broadcast)
    # NOTE: If this table grows > 1GB, we should switch to a Sort-Merge join instead
    merchant_data = [
        ("MERC_99", "Electronics", 0.1),
        ("MERC_01", "Crypto_Exchange", 0.9),
        ("MERC_05", "Grocery", 0.05)
    ]
    merchant_lookup = spark.createDataFrame(merchant_data, ["merchant_id", "category", "risk_factor"])

    # Pulling from scrubbed Silver data
    silver_stream = (spark.readStream
                    .format("delta")
                    .load(PIPELINE_CONFIG['silver_path']))

    # PERFORM JOIN: Broadcast join prevents shuffle tax
    enriched_txns = silver_stream.join(F.broadcast(merchant_lookup), "merchant_id", "left")

    # Business Logic: Score based on category and amount
    fraud_candidates = (enriched_txns
               .withColumn("fraud_score", (F.col("amount") * F.col("risk_factor")) / 100)
               .withColumn("is_fraud", F.when(F.col("fraud_score") > 0.5, True).otherwise(False))
               .withColumn("alert_time", F.current_timestamp())
              )

    print("DEBUG: Active Gold Stream with Broadcast Join...")
    
    return (fraud_candidates.writeStream
             .foreachBatch(upsert_to_gold)
             .option("checkpointLocation", f"{PIPELINE_CONFIG['checkpoint_path']}/gold")
             .start())

if __name__ == "__main__":
    # Ensure Gold table exists first (for the MERGE to work)
    # In a real setup, you'd run a DDL script first.
    print("Starting Gold Layer (Actionable Fraud Alerts)...")
    query = process_gold()
    query.awaitTermination(60)
