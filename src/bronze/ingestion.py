from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from src.utils.config import get_spark_session, PIPELINE_CONFIG

# 1. Define the Raw Schema (Must match Kafka payload)
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("transaction_timestamp", StringType(), True), # ISO string
    StructField("location", StringType(), True)
])

def ingest_to_bronze():
    spark = get_spark_session()
    
    # In a real scenario, use:
    # df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", ...).load()
    
    # For testing, we simulate a Kafka stream using Spark's 'rate' source
    raw_stream = (spark.readStream
                  .format("rate")
                  .option("rowsPerSecond", 5)
                  .load()
                  .withColumn("value", F.to_json(F.struct(
                      F.concat(F.lit("TXN_"), F.col("timestamp").cast("long")).alias("transaction_id"),
                      F.lit("CUST_001").alias("customer_id"),
                      (F.rand() * 1000).alias("amount"),
                      F.lit("USD").alias("currency"),
                      F.lit("MERC_99").alias("merchant_id"),
                      F.lit("4532-XXXX-XXXX-1234").alias("card_number"),
                      F.col("timestamp").cast("string").alias("transaction_timestamp"),
                      F.lit("Mumbai, IN").alias("location")
                  ))) # Simulating a JSON payload from Kafka
                 )

    # 2. Extract JSON and add system metadata
    bronze_df = (raw_stream
                 .withColumn("jsonData", F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA))
                 .select("jsonData.*")
                 .withColumn("ingestion_timestamp", F.current_timestamp())
                 .withColumn("source_file", F.lit("kafka_topic_fintech")) # Metadata for traceability
                )

    # 3. Write to Bronze Delta Table
    query = (bronze_df.writeStream
             .format("delta")
             .outputMode("append")
             .option("checkpointLocation", f"{PIPELINE_CONFIG['checkpoint_path']}/bronze")
             .start(PIPELINE_CONFIG['bronze_path']))
    
    return query

if __name__ == "__main__":
    print("Starting Bronze Ingestion...")
    query = ingest_to_bronze()
    query.awaitTermination(60) # Run for 60 seconds for demo
