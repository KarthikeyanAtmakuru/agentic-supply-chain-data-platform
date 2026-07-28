# agentic-supply-chain-data-platform
Enterprise Azure Databricks Medallion Lakehouse with Vector Search &amp; Agentic Text-to-SQL for Supply Chain Analytics.
## Part 01: Distributed Data Generation & Bronze Layer Ingestion

In this phase, we synthesized 2M+ operational supply chain records using PySpark and Databricks' `dbldatagen` library to simulate raw enterprise ERP and logistics feeds.

### Data Architecture & Generated Entities

| Entity / Table | Record Count | Schema Highlights | Synthetic Realism & Injected Flaws |
| :--- | :--- | :--- | :--- |
| **`dim_warehouses`** | 100 rows | `warehouse_id`, `warehouse_code`, `location`, `max_capacity_units` | Structured SAP-style encoding (`WH-1001` through `WH-1100`). |
| **`dim_products`** | 10,000 rows | `product_id`, `sku`, `category`, `unit_price_usd`, `product_description` | Custom UDFs generating category-specific technical specs for downstream Vector/RAG indexing. |
| **`fact_orders`** | 1,000,000+ rows | `order_id`, `customer_id`, `warehouse_id`, `product_id`, `quantity`, `order_status` | **~2% duplicate records injected** to simulate REST API retry mechanisms. |
| **`fact_shipments`** | 1,000,000 rows | `shipment_id`, `order_id`, `carrier`, `shipment_cost_usd`, `delivery_notes` | **~5% NULL carrier assignments injected** alongside free-text unstructured driver delivery notes. |

### Technical Highlights
- **Distributed Ingestion:** Distributed generation across Spark worker nodes outputting directly to Delta Lake format (`STORAGE_BASE_PATH = "dbfs:/mnt/supply_chain/bronze/"`).
- **Data Quality Baselines:** Deliberately introduced duplicates and missing values to set up transformation testing for the Silver Layer.