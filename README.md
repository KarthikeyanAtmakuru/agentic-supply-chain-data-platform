# agentic-supply-chain-data-platform

A supply chain data platform built on Azure Databricks. The end goal is an AI agent that answers operational questions about orders and shipments in plain English, backed by a clean medallion data architecture.

This README tracks progress through each phase of the build. Currently at Step 5 — agent is live on a Model Serving endpoint.

---

## Part 01: Data Generation and Bronze Ingestion

Used `dbldatagen` to generate 2M+ synthetic supply chain records to simulate raw ERP and logistics data. All records were written as Delta tables to `abd_supplychain_dev.default`.

### Generated Tables

| Table | Rows | Key Columns | Notes |
| :--- | :--- | :--- | :--- |
| `dim_warehouses` | 100 | warehouse_id, warehouse_name, location, max_capacity_units | 5 warehouse names (WH-North through WH-Central), 100 unique IDs |
| `dim_products` | 10,000 | product_id, sku, category, unit_price_usd | 4 categories: Electronics, Industrial, Apparel, Logistics Equipment |
| `fact_orders` | 1,000,000+ | order_id, customer_id, order_status, quantity | ~2% duplicate records injected to simulate retry issues |
| `fact_shipments` | 1,000,000 | shipment_id, order_id, carrier, delivery_notes | ~5% NULL carrier values, free-text delivery notes |

### Bronze Ingestion

The bronze layer reads from the raw tables and adds two audit columns (`_ingested_at`, `_source_file`) before writing to Delta. No transformation happens here, data quality issues are carried through as-is.

Notebook: `docs/Notebooks/02_bronze_ingestion.py`

---

## Part 02: Silver Layer Transformation

Cleaned and deduplicated the bronze layer. Each table gets filtered for NULL keys, deduplicated on its primary key (keeping the most recently ingested row), and written to `abd_supplychain_dev.silver`.

| Silver Table | Key Transformations |
| :--- | :--- |
| `silver_dim_products` | Deduplicated on product_id |
| `silver_dim_warehouses` | Deduplicated on warehouse_id |
| `silver_fact_orders` | Removed ~2% duplicate order_ids, added order_date cast from order_timestamp, filtered NULL keys and zero quantities |
| `silver_fact_shipments` | NULL carriers kept but passed through, validated shipment costs, computed delivery metrics |

Row counts after silver: 10K products, 100 warehouses, 1M orders, 1M shipments.

Notebook: `docs/Notebooks/Silver_Layer_Transformation`

---

## Part 03: Gold Layer Transformation

Built 4 aggregated tables in `abd_supplychain_dev.gold` by joining and aggregating the silver tables. The agent only reads from this layer.

| Gold Table | What it contains |
| :--- | :--- |
| `gold_order_summary` | One row per order, denormalized with product and warehouse info. Includes line_revenue_usd (quantity x unit_price) and is_fulfilled flag |
| `gold_product_revenue` | Aggregated revenue, order counts, and delivered order counts per product/SKU |
| `gold_shipment_performance` | Shipment-level detail with delivery_lead_days and is_on_time flag (based on lead days threshold) |
| `gold_warehouse_operations` | Warehouse throughput, avg order value, total shipments, and on-time delivery % |

Notebook: `docs/Notebooks/Gold_Layer_Transformation`

---

## Data Quality Fix: order_status Bug

After the gold layer was built, we found that `order_status` stored the literal string `{values[-1]}` in every row across all 1M records. This is a `dbldatagen` bug where using `values + weights` on a string column fails to resolve the template and stores the placeholder as plain text instead.

Because `is_fulfilled` in the gold layer was derived as `order_status = 'DELIVERED'`, it was also always false.

**Fix applied:**
- Targeted Delta `UPDATE` on `fact_orders` and `bronze_fact_orders` using a hash-based weighted assignment
- Same `UPDATE` on `silver_fact_orders`
- `MERGE` from silver into `gold_order_summary` (fixes both order_status and is_fulfilled)
- `MERGE` from silver into `gold_product_revenue` (fixes delivered_orders count)
- `gold_shipment_performance.order_status` fixed via MERGE in `03 Register Agent UC Functions` — final distribution: DELIVERED 499K, SHIPPED 300K, PROCESSING 100K, PENDING 51K, CANCELLED 50K

**Generation notebook fix:**
Removed the broken `.withColumn("order_status", "string", values=[...], weights=[...])` line from the `dbldatagen` spec and replaced it with a plain PySpark `withColumn` using `F.rand()` with cumulative thresholds. This matches the pattern already used for `delivery_notes` in the shipments cell.

---

## Part 04: Agent Design (Step 1)

Before writing any code, we defined what the agent should and should not do.

**v1 scope: 2 goals**

| Goal | What it answers | Gold Table |
| :--- | :--- | :--- |
| Order Intelligence | Order status, customer orders, category revenue | `gold_order_summary` |
| Shipment and Delivery | Carrier performance, delays, shipment detail per order | `gold_shipment_performance` |

**Out of scope for v1:** demand forecasting, supplier risk, inventory levels, warehouse capacity, any write operations.

**v2 additions planned:** `gold_product_revenue` (product revenue goal) and `gold_warehouse_operations` (warehouse utilization goal).

Design doc: `Supply Chain Intelligence Agent - Design Doc v1` notebook

---

## Part 05: Tool Design (Step 2)

Each business question from the design doc maps to one callable tool. The rule is: one sample question = one function.

| Tool | Business Question | Inputs | Gold Table |
| :--- | :--- | :--- | :--- |
| `get_order_by_id` | What is the status of order #X? | order_id (BIGINT) | gold_order_summary |
| `get_orders_by_customer` | Which orders does customer #X have? | customer_id (INT) | gold_order_summary |
| `get_orders_by_category` | How is Electronics performing? | category, start_date, end_date | gold_order_summary |
| `get_shipment_by_order` | What happened to the shipment for order #X? | order_id (INT) | gold_shipment_performance |
| `get_carrier_performance` | How is FedEx performing? | carrier (pass NULL for all carriers) | gold_shipment_performance |
| `get_delayed_shipments` | Which shipments are delayed? | max_results (INT) | gold_shipment_performance |

All tools return a JSON string, not a table. The LLM reads JSON naturally; returning a table would require extra serialization.

---

## Part 06: Register Agent Tools as UC Functions (Step 3)

All 6 tools are registered as SQL functions directly in `abd_supplychain_dev.gold`. They sit alongside the gold tables so governance (permissions, lineage, discovery) stays in one place.

The Mosaic AI Agent Framework can auto-discover functions by schema path, so pointing it at `abd_supplychain_dev.gold` is enough to find all 6 tools without any manual wiring.

**Registration notebook:** `docs/Notebooks/03 Register Agent UC Functions`

**Test results:**

| Function | Tested With | Result |
| :--- | :--- | :--- |
| `get_order_by_id` | order_id = 500000 | DELIVERED, Electronics, WH-North, $251.50 |
| `get_orders_by_customer` | customer_id = 1000 | 20+ orders, mix of statuses |
| `get_orders_by_category` | Electronics, full 2025 | 167K orders, $26.3B revenue |
| `get_shipment_by_order` | order_id = 500000 | Fedex Supply, 143 lead days, not on time |
| `get_carrier_performance` | NULL (all carriers) | 5 carriers, ~50% on-time, Xpo fastest at 7.4 avg days |
| `get_delayed_shipments` | max_results = 5 | Top 5 delays, worst at 560 days |

---

---

## Part 07: Build the Agent (Step 4)

Built a LangChain tool-calling agent in `docs/Notebooks/04_build_agent` using a manual `bind_tools` ReAct loop (no LangGraph dependency).

**Stack:**
- **LLM:** `databricks-meta-llama-3-3-70b-instruct` via `ChatDatabricks` (pay-per-token Foundation Model API)
- **Tools:** `UCFunctionToolkit` auto-discovers all functions from `abd_supplychain_dev.gold.*` at runtime
- **Framework:** Manual `bind_tools` loop — avoids LangGraph version conflicts on Serverless compute
- **Tracking:** MLflow tracing (disabled by default; opt-in per cell via `mlflow.langchain.autolog()`)

**7 UC tools (6 original + 1 added in this step):**

| Tool | Purpose |
| :--- | :--- |
| `get_order_by_id` | Order status and detail by order ID |
| `get_orders_by_customer` | All orders for a customer |
| `get_orders_by_category` | Category-level performance over a date range |
| `get_shipment_by_order` | Shipment detail and delivery status for an order |
| `get_carrier_performance` | On-time rate, lead time, cost per carrier |
| `get_delayed_shipments` | Top N most delayed shipments |
| `get_product_revenue` | Revenue, order count, and metrics by product SKU |

**Agent test results (all passing):**

| Question | Answer |
| :--- | :--- |
| Status of order 500000? | DELIVERED, Electronics, WH-North, $251.50 |
| Customer 1000 most recent 5 orders? | Listed with IDs, dates, categories, statuses |
| Electronics orders in 2025? | 167,021 orders, $26.3B total revenue |
| Highest fulfilled category? | Electronics, 125,511 fulfilled orders |
| Shipment for order 500000? | Fedex Supply, 143 lead days, not on time |
| Lowest avg lead time carrier? | Xpo Logistics, 7.4 days |
| 10 most delayed shipments? | Listed, worst = 560 days |
| Revenue for product S2U-ddddd? | $21,950.00 |
| Forecast demand (out of scope)? | Correctly declined |
| Update order status (out of scope)? | Correctly declined |

**Model logged to MLflow:**
- Experiment: `supply-chain-agent`
- Registered model: `abd_supplychain_dev.gold.supply_chain_agent` (version 5, current)
- Pattern: `mlflow.pyfunc.PythonModel` wrapper with explicit `ModelSignature`

Notebook: `docs/Notebooks/04_build_agent`

---

## Part 08: Deploy to Model Serving (Step 5)

Deployed the registered model to a Databricks Model Serving endpoint.

**Endpoint:** `supply-chain-agent`
**URL:** `https://adb-7405604796734154.14.azuredatabricks.net/serving-endpoints/supply-chain-agent/invocations`
**Config:** Small workload, scale-to-zero enabled

**Auth:** The serving container authenticates to Unity Catalog via a PAT injected as `DATABRICKS_TOKEN` from a Databricks secret scope (`supply-chain-agent/databricks-pat`). Both `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are set in `environment_vars` on `ServedModelInput`.

**How to call the endpoint from a notebook:**
```python
from mlflow.deployments import get_deploy_client
client = get_deploy_client("databricks")
resp = client.predict(
    endpoint="supply-chain-agent",
    inputs={"dataframe_records": [{"question": "What is the status of order 500000?"}]},
)
print(resp["predictions"])
```

**Key technical decisions:**
- `mlflow.deployments` used for REST calls (handles auth on all compute types including Serverless)
- UC functions are discovered at inference time via `UCFunctionToolkit(function_names=["abd_supplychain_dev.gold.*"])` — adding new functions to the schema makes them available immediately without re-deploying
- Re-log only needed when wrapper code changes; UC function additions are live automatically
- MLflow tracing disabled by default for clean output; enable per-cell with `mlflow.langchain.autolog()`

---

## What is Next

| Area | Task |
| :--- | :--- |
| Evaluations | Run `mlflow.evaluate` on a golden question set to track answer quality over time |
| UC function descriptions | Add parameter descriptions to all 7 UC functions to improve LLM tool-calling accuracy |
| PAT rotation | Automate PAT refresh before the 90-day expiry (token ID stored in secret scope) |
| Production monitoring | Set up endpoint latency and error-rate alerts via Databricks Jobs |