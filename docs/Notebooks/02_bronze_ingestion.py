import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

# 1. Initialize PySpark with Delta Lake support
spark = SparkSession.builder \
    .appName("AgenticSupplyChain-BronzeIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define input/output paths
RAW_PATH = "data/raw"
BRONZE_PATH = "data/bronze"

# List of tables to ingest
tables = ["dim_products", "dim_warehouses", "fact_orders", "fact_shipments"]

def ingest_to_bronze(table_name: str):
    raw_file = os.path.join(RAW_PATH, f"{table_name}.csv")
    output_dir = os.path.join(BRONZE_PATH, f"bronze_{table_name}")
    
    print(f"Reading raw data from: {raw_file}")
    
    # Read raw CSV
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_file)
    
    # Append audit metadata columns
    bronze_df = df.withColumn("_ingested_at", current_timestamp()) \
                  .withColumn("_source_file", input_file_name())
    
    # Write as Delta format
    print(f"Writing Bronze Delta table to: {output_dir}")
    bronze_df.write.format("delta").mode("overwrite").save(output_dir)
    
    print(f"Successfully ingested bronze_{table_name} ({bronze_df.count()} rows)\n")

if __name__ == "__main__":
    os.makedirs(BRONZE_PATH, exist_ok=True)
    
    for table in tables:
        ingest_to_bronze(table)
        
    print("All raw tables successfully ingested into Bronze Layer!")