# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Distributed Data Generation (Bronze Ingestion)
# MAGIC **Objective:** Generate 1M+ operational supply chain records with injected data quality issues 
# MAGIC and unstructured text blocks, saving as raw Delta tables in the Bronze layer.

# COMMAND -----------

# DBFS / ADLS Gen2 Target Path
STORAGE_BASE_PATH = "dbfs:/mnt/supply_chain/bronze/"

# Import required libraries
import dbldatagen as dg
from faker import Faker
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, IntegerType

fake = Faker()

# UDFs for generating realistic unstructured text blocks for RAG/Vector indexing
@F.udf(returnType=StringType())
def generate_product_description(category):
    specs = {
        "Electronics": f"High-grade micro-controller component. Operating temp: -40C to 85C. Compliance: RoHS compliant. Handling: Sensitive to ESD.",
        "Industrial": f"Heavy-duty hydraulic assembly. Max pressure rating: 5000 PSI. Material: Stainless steel 316. Requires monthly inspection.",
        "Apparel": f"Flame-retardant protective gear. Material: Nomex blend. Size: Standard XL. Certification: ANSI/ISEA 107-2020.",
        "Logistics Equipment": f"Standardized ISO shipping container lock mechanism. Reinforced steel alloy. Weather-resistant seal IP67."
    }
    return specs.get(category, "Standard enterprise supply chain inventory item. Store in dry environment.")

@F.udf(returnType=StringType())
def generate_delivery_notes(status):
    notes = [
        "Package delivered safely to loading dock B. Signed by dock manager.",
        "Carrier delayed by 4 hours due to severe weather conditions on interstate highway.",
        "Inspection flag: Outer packaging showed minor crushing; contents verified intact by receiver.",
        "Rerouted to secondary distribution center due to localized warehouse capacity bottleneck.",
        "Delivery attempted; recipient warehouse closed for holiday. Rescheduled for next business day."
    ]
    return fake.random_element(elements=notes)

# COMMAND -----------

# MAGIC %md
# MAGIC ### 1. Generate `dim_warehouses` (100 rows)

# COMMAND -----------

warehouse_spec = (
    dg.DataGenerator(spark, name="dim_warehouses", rows=100, partitions=2)
    .withId("warehouse_id", start=1, step=1)
    .withOption("warehouse_name", values=["WH-North", "WH-South", "WH-East", "WH-West", "WH-Central"])
    .withOption("location", values=["Dallas, TX", "Chicago, IL", "Seattle, WA", "Atlanta, GA", "Columbus, OH"])
    .withColumn("max_capacity_units", "integer", minValue=50000, maxValue=500000)
)

df_warehouses = warehouse_spec.build()
df_warehouses.write.format("delta").mode("overwrite").save(f"{STORAGE_BASE_PATH}/dim_warehouses")
print("Saved dim_warehouses to Bronze Lakehouse.")

# COMMAND -----------

# MAGIC %md
# MAGIC ### 2. Generate `dim_products` (10,000 rows)

# COMMAND -----------

product_spec = (
    dg.DataGenerator(spark, name="dim_products", rows=10000, partitions=4)
    .withId("product_id", start=1001, step=1)
    .withColumn("sku", "string", template="SKU-\\\\d\\\\d\\\\d\\\\d\\\\d")
    .withOption("category", values=["Electronics", "Industrial", "Apparel", "Logistics Equipment"])
    .withColumn("unit_price_usd", "double", minValue=15.50, maxValue=1250.00, random=True)
)

df_products = product_spec.build().withColumn("product_description", generate_product_description(F.col("category")))
df_products.write.format("delta").mode("overwrite").save(f"{STORAGE_BASE_PATH}/dim_products")
print("Saved dim_products to Bronze Lakehouse.")

# COMMAND -----------

# MAGIC %md
# MAGIC ### 3. Generate `fact_orders` (1,000,000 rows with duplicates & edge cases)

# COMMAND -----------

orders_spec = (
    dg.DataGenerator(spark, name="fact_orders", rows=1000000, partitions=8)
    .withId("order_id", start=500000, step=1)
    .withColumn("customer_id", "integer", minValue=1000, maxValue=50000)
    .withColumn("warehouse_id", "integer", minValue=1, maxValue=100)
    .withColumn("product_id", "integer", minValue=1001, maxValue=11000)
    .withColumn("quantity", "integer", minValue=1, maxValue=500)
    .withOption("order_status", values=["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"], weights=[5, 10, 30, 50, 5])
    .withColumn("order_timestamp", "timestamp", begin="2025-01-01 00:00:00", end="2026-06-30 23:59:59", random=True)
)

df_orders = orders_spec.build()

# Inject ~2% duplicate order records to simulate API ingestion retry issues
df_duplicates = df_orders.sample(withReplacement=False, fraction=0.02)
df_orders_raw = df_orders.union(df_duplicates)

df_orders_raw.write.format("delta").mode("overwrite").save(f"{STORAGE_BASE_PATH}/fact_orders")
print("Saved fact_orders (with duplicates) to Bronze Lakehouse.")

# COMMAND -----------

# MAGIC %md
# MAGIC ### 4. Generate `fact_shipments` (1,000,000 rows with nulls & unstructured text)

# COMMAND -----------

shipments_spec = (
    dg.DataGenerator(spark, name="fact_shipments", rows=1000000, partitions=8)
    .withId("shipment_id", start=900000, step=1)
    .withColumn("order_id", "integer", minValue=500000, maxValue=1500000)
    .withOption("carrier", values=["FedEx Supply", "UPS Freight", "DHL Express", "Amazon Logistics", "XPO Logistics"])
    .withColumn("shipment_cost_usd", "double", minValue=45.00, maxValue=2500.00, random=True)
    .withColumn("ship_timestamp", "timestamp", begin="2025-01-02 00:00:00", end="2026-07-15 23:59:59", random=True)
)

df_shipments = shipments_spec.build()

# Inject ~5% null carrier values (simulating pending assignment) and generate unstructured delivery notes
df_shipments = (
    df_shipments
    .withColumn("carrier", F.when(F.rand() < 0.05, F.lit(None)).otherwise(F.col("carrier")))
    .withColumn("delivery_notes", generate_delivery_notes(F.lit("DELIVERED")))
)

df_shipments.write.format("delta").mode("overwrite").save(f"{STORAGE_BASE_PATH}/fact_shipments")
print("Saved fact_shipments (with nulls & unstructured notes) to Bronze Lakehouse.")