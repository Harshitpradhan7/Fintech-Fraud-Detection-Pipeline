import os
from pyspark.sql import SparkSession

# DETECT ENVIRONMENT
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

def get_spark_session(app_name="FraudDetectionPipeline"):
    if IS_DATABRICKS:
        from databricks.sdk.runtime import spark
        return spark
    else:
        # Local Windows/Linux Spark environment
        base_path = os.path.abspath(".")
        if os.name == 'nt':
            os.environ["HADOOP_HOME"] = base_path
            os.environ["PATH"] = os.environ["PATH"] + ";" + os.path.join(base_path, "bin")
        
        warehouse_uri = "file:///" + os.path.join(base_path, "spark-warehouse").replace("\\", "/")
            
        return (SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.driver.host", "localhost")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.warehouse.dir", warehouse_uri)
                .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate())

# PATH CONFIGURATION
if IS_DATABRICKS:
    # Cloud Paths (Unity Catalog or DBFS)
    PIPELINE_CONFIG = {
        "checkpoint_path": "dbfs:/fraud_pipeline/checkpoints",
        "bronze_path": "dbfs:/fraud_pipeline/bronze",
        "silver_path": "dbfs:/fraud_pipeline/silver",
        "gold_path": "dbfs:/fraud_pipeline/gold",
        "kafka_bootstrap_servers": "YOUR_KAFKA_BROKER:9092",
        "kafka_topic": "fintech_transactions"
    }
else:
    # Local Paths
    PIPELINE_CONFIG = {
        "checkpoint_path": "data/checkpoints",
        "bronze_path": "data/bronze",
        "silver_path": "data/silver",
        "gold_path": "data/gold",
        "kafka_bootstrap_servers": "localhost:9092",
        "kafka_topic": "fintech_transactions"
    }
