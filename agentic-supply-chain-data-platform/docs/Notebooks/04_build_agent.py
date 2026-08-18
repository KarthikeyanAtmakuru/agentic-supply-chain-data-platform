# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Step 4 — Build the Supply Chain Intelligence Agent
# MAGIC %md
# MAGIC # Step 4 — Build the Supply Chain Intelligence Agent
# MAGIC
# MAGIC Builds a LangChain agent backed by the 6 UC functions registered in `abd_supplychain_dev.gold`.
# MAGIC
# MAGIC Stack:
# MAGIC - **LLM**: `databricks-meta-llama-3-3-70b-instruct` via `ChatDatabricks`
# MAGIC - **Tools**: `UCFunctionToolkit` auto-discovers the 6 UC functions by schema path
# MAGIC - **Framework**: LangChain tool-calling agent + AgentExecutor
# MAGIC - **Tracking**: MLflow auto-logging captures every tool call and LLM step
# MAGIC
# MAGIC The agent only answers questions in v1 scope (orders and shipments). It says it does not know for anything outside that scope.

# COMMAND ----------

# DBTITLE 1,Install packages
# Removed top-level 'langchain' — it pulls in a langchain.agents import that
# requires ExecutionInfo from langgraph.runtime, which does not exist in all versions.
# Using langgraph.prebuilt.create_react_agent directly avoids the conflict entirely.
# Install databricks-langchain with --no-deps to prevent overwriting
# the pre-installed Databricks mlflow (causes circular import otherwise).
# langchain-core is installed separately for message primitives.
# No langgraph: using a manual bind_tools loop instead to avoid version conflicts.
%pip install -q --no-deps databricks-langchain
%pip install -q langchain-core
dbutils.library.restartPython()


# COMMAND ----------

# DBTITLE 1,Imports
import mlflow
from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

# Tracing off by default — clean single-line answers everywhere.
# To see the MLflow Trace UI for a specific cell, call mlflow.langchain.autolog()
# at the top of that cell (e.g. the test cells below).
mlflow.tracing.disable()

print(f"mlflow     : {mlflow.__version__}")
print("Imports OK")

# COMMAND ----------

# DBTITLE 1,Connect to LLM endpoint
# Using the Databricks Foundation Model API
# The endpoint is pay-per-token and available by default in every Databricks workspace
llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    temperature=0.0,       # deterministic responses for factual queries
    max_tokens=2048,
)

# Quick sanity check
response = llm.invoke("Reply with just the word: ready")
print(f"LLM status: {response.content}")

# COMMAND ----------

# DBTITLE 1,Load UC tools from gold schema
# UCFunctionToolkit discovers all functions registered in abd_supplychain_dev.gold
# This includes all 6 agent tools we registered in Step 3
toolkit = UCFunctionToolkit(
    function_names=["abd_supplychain_dev.gold.*"]
)
tools = toolkit.tools

print(f"Tools loaded: {len(tools)}")
for t in tools:
    print(f"  - {t.name}")

# COMMAND ----------

# DBTITLE 1,Register get_product_revenue UC function
# gold_product_revenue already has per-SKU metrics — just need a UC function to expose it.
spark.sql("""
CREATE OR REPLACE FUNCTION abd_supplychain_dev.gold.get_product_revenue(sku_input STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Returns total revenue, order count, and key metrics for a product by SKU.
          Pass the exact SKU string (e.g. S2U-ddddd). Case-insensitive match.'
RETURN (
    SELECT to_json(struct(
        product_id,
        sku,
        category,
        price_tier,
        unit_price_usd,
        total_orders,
        total_quantity_sold,
        total_revenue_usd,
        avg_revenue_per_order_usd,
        avg_quantity_per_order,
        delivered_orders
    ))
    FROM abd_supplychain_dev.gold.gold_product_revenue
    WHERE upper(sku) = upper(sku_input)
    LIMIT 1
)
""")
print("Function registered: abd_supplychain_dev.gold.get_product_revenue")

# COMMAND ----------

# DBTITLE 1,Write the system prompt
SYSTEM_PROMPT = """
You are a supply chain intelligence assistant for an operations team.
You answer questions about orders and shipments using the tools available to you.

What you can help with:
- Looking up a specific order by order ID
- Finding all orders for a customer
- Summarising order performance for a product category over a date range
- Getting shipment and delivery details for a specific order
- Checking carrier performance metrics (on-time rates, lead times, costs)
- Listing delayed shipments

What you cannot help with:
- Demand forecasting or inventory planning
- Supplier risk or supplier data
- Warehouse capacity or utilisation
- Modifying or updating any data

How to use your tools:
- Always call a tool to get data before answering. Do not guess or make up numbers.
- For order questions, use get_order_by_id or get_orders_by_customer.
- For category performance, use get_orders_by_category with a date range in YYYY-MM-DD format.
- For shipment questions on a specific order, use get_shipment_by_order.
- For carrier comparisons, use get_carrier_performance. Pass NULL to get all carriers at once.
- For delayed shipments, use get_delayed_shipments with a sensible max_results (10-20).
- For revenue, order count, or metrics for a specific product SKU, use get_product_revenue.
- Known product categories: Electronics, Industrial, Apparel, Logistics Equipment
- Known carriers: Fedex Supply, Ups Freight, Dhl Express, Amazon Logistics, Xpo Logistics
- Data date range: 2025-01-01 to 2026-06-30. Always use dates within this range when calling get_orders_by_category. Do not use dates outside this range.

When you cannot answer, say clearly what is out of scope instead of guessing.
Keep answers concise and factual. If the tool returns an empty result, say so.
"""

# SYSTEM_PROMPT is passed directly to run_agent via SystemMessage
# No ChatPromptTemplate needed with the bind_tools approach
print("System prompt ready")

# COMMAND ----------

# DBTITLE 1,Build the agent
# Bind the tools to the LLM so it knows what functions it can call.
llm_with_tools = llm.bind_tools(tools)

# Build a name -> tool mapping for fast lookup during the loop.
tool_map = {t.name: t for t in tools}

def run_agent(question: str, max_iterations: int = 5) -> str:
    """
    Simple ReAct-style agent loop.
    1. Send messages to the LLM.
    2. If the LLM returns tool calls, execute them and append results.
    3. Loop until the LLM returns a plain text answer or max_iterations reached.
    """
    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=question),
    ]

    for step in range(max_iterations):
        response = llm_with_tools.invoke(messages)
        messages.append(response)

        # No tool calls = final answer
        if not response.tool_calls:
            return response.content

        # Execute each tool call; on error, return the error message to the LLM
        # so it can self-correct its arguments on the next iteration.
        for tc in response.tool_calls:
            tool = tool_map.get(tc["name"])
            if not tool:
                result = f"Unknown tool: {tc['name']}"
            else:
                try:
                    result = tool.invoke(tc["args"])
                except Exception as e:
                    result = f"Tool call failed: {e}. Check the parameter types and try again with correct values."
            messages.append(ToolMessage(content=str(result), tool_call_id=tc["id"]))

    # Fallback if max iterations hit
    return messages[-1].content if messages else "No response."

print("Agent ready")

# COMMAND ----------

# DBTITLE 1,Test — Sample questions from the design doc
# MAGIC %md
# MAGIC ## Testing the Agent
# MAGIC
# MAGIC Running through the 10 sample questions defined in the design doc.
# MAGIC Each question exercises a different tool and tests a different answer pattern.

# COMMAND ----------

# DBTITLE 1,Test — Order questions
mlflow.langchain.autolog()  # enable trace UI for this cell only

order_questions = [
    "What is the status of order 500000?",
    "Which orders does customer 1000 have? Show me the most recent 5.",
    "How many Electronics orders were placed in 2025 and what was the total revenue?",
    "Which product category has the highest number of fulfilled orders?",
]

for q in order_questions:
    print(f"\nQ: {q}")
    print("-" * 60)
    answer = run_agent(q)
    print(f"A: {answer}")
    print()

# COMMAND ----------

# DBTITLE 1,Test — Shipment questions
shipment_questions = [
    "What happened to the shipment for order 500000? Was it on time?",
    "What is the on-time delivery rate for each carrier?",
    "Which carrier has the lowest average delivery lead time?",
    "Show me the 10 most delayed shipments.",
    "What is the total shipment cost for Fedex Supply?",
]

for q in shipment_questions:
    print(f"\nQ: {q}")
    print("-" * 60)
    answer = run_agent(q)
    print(f"A: {answer}")
    print()

# COMMAND ----------

# DBTITLE 1,Test — Out of scope question
# Verify the agent correctly declines out-of-scope requests
out_of_scope = [
    "Can you forecast demand for Electronics next quarter?",
    "Update the status of order 500000 to DELIVERED.",
]

for q in out_of_scope:
    print(f"\nQ: {q}")
    print("-" * 60)
    answer = run_agent(q)
    print(f"A: {answer}")
    print()

# COMMAND ----------

# DBTITLE 1,Log the agent with MLflow
# PyFunc wrapper — rebuilds the LLM and tools at inference time.
# DatabricksFunctionClient uses the serving endpoint's injected credentials
# (set via environment_vars in the deploy cell below — see DATABRICKS_TOKEN).
class SupplyChainAgentWrapper(mlflow.pyfunc.PythonModel):
    """MLflow PyFunc wrapper for the Supply Chain Intelligence Agent."""

    def predict(self, context, model_input):
        import os
        from databricks_langchain import ChatDatabricks, UCFunctionToolkit, set_uc_function_client
        from databricks_langchain import DatabricksFunctionClient
        from databricks.sdk import WorkspaceClient
        from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

        system_prompt = context.model_config.get("system_prompt", "")

        # Build a WorkspaceClient from env vars injected by the serving endpoint.
        # Both DATABRICKS_HOST and DATABRICKS_TOKEN are set via environment_vars
        # in the deploy cell. The host falls back to the workspace URL in case
        # the env var is missing (e.g. older endpoint config).
        _host = os.environ.get(
            "DATABRICKS_HOST",
            "https://adb-7405604796734154.14.azuredatabricks.net",
        )
        _token = os.environ["DATABRICKS_TOKEN"]
        _ws = WorkspaceClient(host=_host, token=_token)
        _uc_client = DatabricksFunctionClient(workspace_client=_ws)
        set_uc_function_client(_uc_client)

        _llm = ChatDatabricks(
            endpoint="databricks-meta-llama-3-3-70b-instruct",
            temperature=0.0, max_tokens=2048,
        )
        _toolkit = UCFunctionToolkit(function_names=["abd_supplychain_dev.gold.*"])
        _tools = _toolkit.tools
        _llm_with_tools = _llm.bind_tools(_tools)
        _tool_map = {t.name: t for t in _tools}

        question = model_input["question"].iloc[0] if hasattr(model_input, "iloc") else str(model_input)
        messages = [SystemMessage(content=system_prompt), HumanMessage(content=question)]
        for _ in range(5):
            response = _llm_with_tools.invoke(messages)
            messages.append(response)
            if not response.tool_calls:
                return response.content
            for tc in response.tool_calls:
                tool = _tool_map.get(tc["name"])
                try:
                    result = tool.invoke(tc["args"]) if tool else f"Unknown tool: {tc['name']}"
                except Exception as e:
                    result = f"Tool error: {e}"
                messages.append(ToolMessage(content=str(result), tool_call_id=tc["id"]))
        return messages[-1].content if messages else "No response."


mlflow.set_experiment("/Users/karthik.atmakuru425@gmail.com/agentic-supply-chain-data-platform/supply-chain-agent")

from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

signature = ModelSignature(
    inputs=Schema([ColSpec("string", "question")]),
    outputs=Schema([ColSpec("string", "answer")]),
)

with mlflow.start_run(run_name="supply_chain_agent_v5"):  # current deployed version
    model_info = mlflow.pyfunc.log_model(
        artifact_path="supply_chain_agent",
        python_model=SupplyChainAgentWrapper(),
        model_config={"system_prompt": SYSTEM_PROMPT},
        signature=signature,        # explicit signature — skips slow input_example validation
        registered_model_name="abd_supplychain_dev.gold.supply_chain_agent",
    )

print(f"Model logged : {model_info.model_uri}")
print("Registered   : abd_supplychain_dev.gold.supply_chain_agent")

# COMMAND ----------

# DBTITLE 1,Create secret scope and PAT for serving
# Creates everything the serving endpoint needs for UC auth in one cell.
# The PAT value is NEVER printed — it goes straight from SDK to secret store.
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# 1 — Create secret scope (safe to rerun; skips if already exists)
try:
    w.secrets.create_scope(scope="supply-chain-agent")
    print("Secret scope created: supply-chain-agent")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Secret scope already exists: supply-chain-agent")
    else:
        raise

# 2 — Create a 90-day PAT for the serving endpoint to use
token_resp = w.tokens.create(
    comment="supply-chain-agent-serving",
    lifetime_seconds=90 * 24 * 60 * 60,  # 90 days
)

# 3 — Store PAT in the secret scope (value never leaves this cell)
w.secrets.put_secret(
    scope="supply-chain-agent",
    key="databricks-pat",
    string_value=token_resp.token_value,
)

print(f"PAT stored  : supply-chain-agent/databricks-pat")
print(f"Token ID    : {token_resp.token_info.token_id}")
print(f"Expires     : {token_resp.token_info.expiry_time}")

# COMMAND ----------

# DBTITLE 1,Deploy to Model Serving endpoint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedModelInput
import datetime

w = WorkspaceClient()
ENDPOINT_NAME = "supply-chain-agent"

# Check whether the endpoint already exists
existing = None
try:
    existing = w.serving_endpoints.get(name=ENDPOINT_NAME)
except Exception:
    pass

MODEL_VERSION = "5"  # current live version — bump only when wrapper code changes

# environment_vars goes inside ServedModelInput so it applies to both
# create and update paths. The secret reference is resolved at serve time.
served_model = ServedModelInput(
    model_name="abd_supplychain_dev.gold.supply_chain_agent",
    model_version=MODEL_VERSION,
    workload_size="Small",
    scale_to_zero_enabled=True,
    environment_vars={
        "DATABRICKS_HOST": "https://adb-7405604796734154.14.azuredatabricks.net",
        "DATABRICKS_TOKEN": "{{secrets/supply-chain-agent/databricks-pat}}",
    },
)
new_config = EndpointCoreConfigInput(served_models=[served_model])

if existing:
    print(f"Endpoint exists — updating to model version {MODEL_VERSION}...")
    endpoint = w.serving_endpoints.update_config_and_wait(
        name=ENDPOINT_NAME,
        served_models=new_config.served_models,
        timeout=datetime.timedelta(minutes=15),
    )
else:
    print(f"Creating endpoint '{ENDPOINT_NAME}' — typically takes 5-10 minutes...")
    endpoint = w.serving_endpoints.create_and_wait(
        name=ENDPOINT_NAME,
        config=new_config,
        timeout=datetime.timedelta(minutes=15),
    )
print(f"Endpoint ready — state: {endpoint.state.ready}")

print(f"\nInvocation URL:")
print(f"{w.config.host}/serving-endpoints/{ENDPOINT_NAME}/invocations")

# COMMAND ----------

# DBTITLE 1,How to use the registered model
# MAGIC %md
# MAGIC ## How to use the registered model
# MAGIC
# MAGIC ### Option 1 — Load in a notebook (testing / batch)
# MAGIC Load the model by UC path and call `.predict()` with a DataFrame.
# MAGIC No LLM endpoint credentials needed — the wrapper handles auth automatically.
# MAGIC
# MAGIC ### Option 2 — Deploy to a Model Serving endpoint (production API)
# MAGIC 1. Open the registered model in Unity Catalog: `abd_supplychain_dev` → `gold` → `supply_chain_agent`
# MAGIC 2. Click **Serve this model** → choose **Real-time** → select version 5
# MAGIC 3. Databricks creates a REST endpoint. Call it with any HTTP client.

# COMMAND ----------

# DBTITLE 1,Option 1 — Load and call the model in a notebook
import mlflow
import pandas as pd

# Load version 5 (current deployed version)
# Use "latest" or a specific version number, or an alias like @champion
loaded_model = mlflow.pyfunc.load_model(
    "models:/abd_supplychain_dev.gold.supply_chain_agent/5"
)

# Call it — input must be a DataFrame with a 'question' column
test_questions = [
    "What is the status of order 500000?",
    "Which carrier has the lowest average delivery lead time?",
]

for q in test_questions:
    result = loaded_model.predict(pd.DataFrame({"question": [q]}))
    print(f"Q: {q}")
    print(f"A: {result}")
    print()

# COMMAND ----------

# DBTITLE 1,Option 2 — Call the model via REST API (after deploying to serving)
# mlflow.deployments handles auth automatically on all compute types
# (Serverless, Classic clusters, jobs) — no token wrangling needed.
from mlflow.deployments import get_deploy_client
import json

ENDPOINT_NAME = "supply-chain-agent"
client = get_deploy_client("databricks")

test_questions = [
    "What is the status of order 500000?",                          # order lookup
    "Find the total revenue for the product S2U-ddddd",             # new tool
    "Can you forecast demand for Electronics next quarter?",         # out of scope
]

for q in test_questions:
    resp = client.predict(
        endpoint=ENDPOINT_NAME,
        inputs={"dataframe_records": [{"question": q}]},
    )
    print(f"Q: {q}")
    print(f"A: {resp['predictions']}")
    print()

# COMMAND ----------

# DBTITLE 1,Ask the agent a question
# Change the question below to ask anything in scope
print(run_agent("find the total revenue for the product S2U-ddddd"))

# COMMAND ----------

# DBTITLE 1,Ask the agent another question
print(run_agent("find the total revenue for electronic products"))