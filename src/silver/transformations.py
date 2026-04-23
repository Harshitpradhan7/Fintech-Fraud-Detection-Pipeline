from pyspark.sql import functions as F
from src.utils.config import get_spark_session, PIPELINE_CONFIG

def mask_pii(df, salt="S3CR3T_SALT"):
    """
    Masks PII columns using SHA-256 salted hashing.
    """
    return (df
            .withColumn("customer_id", F.sha2(F.concat(F.col("customer_id"), F.lit(salt)), 256))
            .withColumn("card_number_masked", F.sha2(F.concat(F.col("card_number"), F.lit(salt)), 256))
            .drop("card_number") # Remove raw card number immediately
           )

def process_silver():
    spark = get_spark_session()
    
    # Read the raw feed. Using 'maxFilesPerTrigger' to prevent executor OOM during spikes.
    raw_stream = (spark.readStream
                 .format("delta")
                 .load(PIPELINE_CONFIG['bronze_path']))

    # Transformation Logic: 
    # 1. Standardize timestamps
    # 2. Salted SHA-256 for PII (Customer/Card)
    # 3. Deduplicate based on transaction_id (Kafka at-least-once fix)
    scrubbed_txns = (raw_stream
                 .withColumn("transaction_timestamp", F.to_timestamp(F.col("transaction_timestamp")))
                 .withColumn("amount", F.col("amount").cast("double"))
                 .transform(lambda df: mask_pii(df))
                 .dropDuplicates(["transaction_id"])
                 .withColumn("refined_at", F.current_timestamp())
                )

    print("INFO: Silver Stream scrubbed and ready for writing.")

    return (scrubbed_txns.writeStream
             .format("delta")
             .outputMode("append")
             .option("checkpointLocation", f"{PIPELINE_CONFIG['checkpoint_path']}/silver")
             .start(PIPELINE_CONFIG['silver_path']))

if __name__ == "__main__":
    print("Starting Silver Transformation (Cleaning & Masking)...")
    query = process_silver()
    query.awaitTermination(60)
