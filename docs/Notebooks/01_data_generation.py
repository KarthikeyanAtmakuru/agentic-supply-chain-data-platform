# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "dbldatagen",
#   "faker",
# ]
# ///
# MAGIC %pip install dbldatagen faker

# COMMAND ----------

#dbutils.library.restartPython()

# COMMAND ----------

# Import required libraries
import dbldatagen as dg
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Generate `dim_warehouses` (100 rows)

# COMMAND ----------

warehouse_spec = (
    dg.DataGenerator(spark, name="dim_warehouses", rows=100, partitions=2)
    .withColumn("warehouse_id", "integer", minValue=1, maxValue=100, step=1)
    .withColumn("warehouse_name", "string", values=["WH-North", "WH-South", "WH-East", "WH-West", "WH-Central"])
    .withColumn("location", "string", values=["Dallas, TX", "Chicago, IL", "Seattle, WA", "Atlanta, GA", "Columbus, OH"])
    .withColumn("max_capacity_units", "integer", minValue=50000, maxValue=500000)
)

df_warehouses = warehouse_spec.build()

# Saved directly as a managed Delta table in default catalog/schema
df_warehouses.write.format("delta").mode("overwrite").saveAsTable("default.dim_warehouses")
print("Successfully saved dim_warehouses as managed table 'default.dim_warehouses'!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Generate `dim_products` (10,000 rows)

# COMMAND ----------

product_spec = (
    dg.DataGenerator(spark, name="dim_products", rows=10000, partitions=4)
    .withColumn("product_id", "integer", minValue=1001, maxValue=11000, step=1)
    .withColumn("sku", "string", template="SKU-\\d\\d\\d\\d\\d")
    .withColumn("category", "string", values=["Electronics", "Industrial", "Apparel", "Logistics Equipment"])
    .withColumn("unit_price_usd", "double", minValue=15.50, maxValue=1250.00, random=True)
)

# Pure PySpark expression (No UDF = No Serialization Errors!)
df_products = product_spec.build().withColumn(
    "product_description",
    F.when(F.col("category") == "Electronics", "High-grade micro-controller component. Operating temp: -40C to 85C. Compliance: RoHS compliant. Handling: Sensitive to ESD.")
     .when(F.col("category") == "Industrial", "Heavy-duty hydraulic assembly. Max pressure rating: 5000 PSI. Material: Stainless steel 316. Requires monthly inspection.")
     .when(F.col("category") == "Apparel", "Flame-retardant protective gear. Material: Nomex blend. Size: Standard XL. Certification: ANSI/ISEA 107-2020.")
     .when(F.col("category") == "Logistics Equipment", "Standardized ISO shipping container lock mechanism. Reinforced steel alloy. Weather-resistant seal IP67.")
     .otherwise("Standard enterprise supply chain inventory item. Store in dry environment.")
)

df_products.write.format("delta").mode("overwrite").saveAsTable("default.dim_products")
print("Saved dim_products as managed table 'default.dim_products'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Generate `fact_orders` (1,000,000 rows with duplicates)

# COMMAND ----------

# DBTITLE 1,Generate fact_orders (1,000,000 rows with duplicates)
orders_spec = (
    dg.DataGenerator(spark, name="fact_orders", rows=1000000, partitions=8)
    .withColumn("order_id", "long", minValue=500000, maxValue=1500000, step=1)
    .withColumn("customer_id", "integer", minValue=1000, maxValue=50000)
    .withColumn("warehouse_id", "integer", minValue=1, maxValue=100)
    .withColumn("product_id", "integer", minValue=1001, maxValue=11000)
    .withColumn("quantity", "integer", minValue=1, maxValue=500)
    .withColumn("order_timestamp", "timestamp", begin="2025-01-01 00:00:00", end="2026-06-30 23:59:59", random=True)
)

df_orders = orders_spec.build()

# Fix: dbldatagen values+weights has a template resolution bug on string columns
# (stores literal '{values[-1]}' instead of sampling from the list)
# Generate order_status using pure PySpark weighted random selection instead
# Distribution: PENDING 5%, PROCESSING 10%, SHIPPED 30%, DELIVERED 50%, CANCELLED 5%
df_orders = (
    df_orders
    .withColumn("_r", F.rand())
    .withColumn(
        "order_status",
        F.when(F.col("_r") < 0.05,  "PENDING")
         .when(F.col("_r") < 0.15,  "PROCESSING")
         .when(F.col("_r") < 0.45,  "SHIPPED")
         .when(F.col("_r") < 0.95,  "DELIVERED")
         .otherwise("CANCELLED")
    )
    .drop("_r")
)

# Inject ~2% duplicate order records
df_duplicates = df_orders.sample(withReplacement=False, fraction=0.02)
df_orders_raw = df_orders.union(df_duplicates)

df_orders_raw.write.format("delta").mode("overwrite").saveAsTable("default.fact_orders")
print("Saved fact_orders as managed table 'default.fact_orders'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Generate `fact_shipments` (1,000,000 rows with nulls & unstructured text)

# COMMAND ----------

shipments_spec = (
    dg.DataGenerator(spark, name="fact_shipments", rows=1000000, partitions=8)
    .withColumn("shipment_id", "long", minValue=900000, maxValue=1900000, step=1)
    .withColumn("order_id", "integer", minValue=500000, maxValue=1500000)
    .withColumn("carrier", "string", values=["FedEx Supply", "UPS Freight", "DHL Express", "Amazon Logistics", "XPO Logistics"])
    .withColumn("shipment_cost_usd", "double", minValue=45.00, maxValue=2500.00, random=True)
    .withColumn("ship_timestamp", "timestamp", begin="2025-01-02 00:00:00", end="2026-07-15 23:59:59", random=True)
)

df_shipments = shipments_spec.build()

# Pure PySpark expression for selecting delivery notes
notes_array = F.array(
    F.lit("Package delivered safely to loading dock B. Signed by dock manager."),
    F.lit("Carrier delayed by 4 hours due to severe weather conditions on interstate highway."),
    F.lit("Inspection flag: Outer packaging showed minor crushing; contents verified intact by receiver."),
    F.lit("Rerouted to secondary distribution center due to localized warehouse capacity bottleneck."),
    F.lit("Delivery attempted; recipient warehouse closed for holiday. Rescheduled for next business day.")
)

df_shipments = (
    df_shipments
    .withColumn("carrier", F.when(F.rand() < 0.05, F.lit(None)).otherwise(F.col("carrier")))
    .withColumn("delivery_notes", F.element_at(notes_array, (F.floor(F.rand() * 5) + 1).cast("int")))
)

df_shipments.write.format("delta").mode("overwrite").saveAsTable("default.fact_shipments")
print("Saved fact_shipments as managed table 'default.fact_shipments'.")