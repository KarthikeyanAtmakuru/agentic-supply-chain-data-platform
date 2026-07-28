# Functional Design Document: Agentic Enterprise Supply Chain Data Platform

## 1. Executive Summary & Business Objective
- **Business Problem:** Enterprise supply chain analysts and logistics managers frequently require real-time visibility into order fulfillment delays, inventory bottlenecks, and carrier SLA compliance across global warehouses. However, querying multi-tier relational warehouses requires writing complex PySpark/SQL joins that non-technical stakeholders cannot easily write.
- **Proposed Solution:** Architect a modern, production-grade Azure Databricks platform featuring:
  1. A multi-tier Medallion Lakehouse (Bronze -> Silver -> Gold) processing high-volume supply chain transactions with strict ACID guarantees via Delta Lake.
  2. A Vector-Indexed Metadata & Catalog Layer extracting semantic context from catalog descriptions and carrier logs.
  3. An Agentic AI Interface translating natural-language supply chain questions into safe, optimized Databricks SQL queries executed via automated tools.

## 2. High-Level Architecture & Data Flow
1. **Raw Ingestion (Bronze Layer):** 1M+ synthetic operational records (Orders, Shipments, Products, Warehouses) ingested as raw Delta tables on Azure ADLS Gen2.
2. **Transformation & Cleansing (Silver Layer):** PySpark pipelines enforce schema validation, deduplicate transaction logs, drop/impute null values, and structure relational primary/foreign keys.
3. **Business Aggregation (Gold Layer):** Materialized star-schema views optimized for downstream analytical workloads and metadata indexing.
4. **Vector Search & RAG:** Catalog schemas and unstructured shipment text generated into text embeddings and stored in a Databricks Vector Search Index.
5. **Agentic Execution:** LangChain / LlamaIndex agent receives user prompt -> queries Vector Index for schema context -> constructs safe SQL -> executes against Databricks SQL Endpoint -> returns answer with visual metrics.

## 3. Data Ingestion & Data Generation Specification
- **Engine:** PySpark `dbldatagen` + `Faker` executed within Azure Databricks notebooks.
- **Target Volume:** 1,000,000+ transaction records.
- **Schema Entities:**
  - `dim_warehouses`: Warehouse metadata, location, capacity constraints.
  - `dim_products`: SKU categories, pricing, and unstructured text descriptions.
  - `fact_orders`: Transactional order logs, customer IDs, timestamps, order statuses.
  - `fact_shipments`: Carrier tracking, shipment dates, and unstructured delivery logs.
- **Data Quality Injections:** Intentionally includes missing fields (5% nulls in carrier assignments), duplicate API events (2% duplicates in order IDs), string formatting inconsistencies, and unstructured natural language text blocks.