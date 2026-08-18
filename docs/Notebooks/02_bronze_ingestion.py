import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Initialize Spark (Databricks manages the catalog session automatically)
spark = SparkSession.builder.appName("AgenticSupplyChain-BronzeIngestion").getOrCreate()

CATALOG = "abd_supplychain_dev"
SCHEMA = "default"

tables = ["dim_products", "dim_warehouses", "fact_orders", "fact_shipments"]

def ingest_to_bronze(table_name: str):
    source_table = f"{CATALOG}.{SCHEMA}.{table_name}"
    target_table = f"{CATALOG}.{SCHEMA}.bronze_{table_name}"
    
    print(f"Reading raw data from catalog table: {source_table}")
    
    # Read directly from Unity Catalog
    df = spark.read.table(source_table)
    
    # Append audit metadata columns
    bronze_df = df.withColumn("_ingested_at", current_timestamp()) \
                  .withColumn("_source_file", lit(source_table))
    
    # Write directly as a Delta table in Unity Catalog
    print(f"Writing Bronze Delta table to: {target_table}")
    bronze_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    
    print(f"Successfully ingested {target_table} ({bronze_df.count()} rows)\n")

if __name__ == "__main__":
    for table in tables:
        ingest_to_bronze(table)
        
    print("All raw tables successfully ingested into Bronze Layer!")