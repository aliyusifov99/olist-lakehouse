# Databricks notebook source
# MAGIC %md
# MAGIC # SDP Bronze Layer — Raw CSV Ingestion via Auto Loader
# MAGIC
# MAGIC Recreates Phase 1's Bronze ingestion as a Lakeflow Spark Declarative Pipeline.
# MAGIC Each of the 9 source folders becomes a streaming table in the pipeline's
# MAGIC target schema (`dlt_output`, configured at pipeline level in pahse 4).
# MAGIC
# MAGIC ## Key differences from Phase 1's `01_bronze_ingestion`
# MAGIC
# MAGIC | Aspect | Phase 1 (imperative) | Phase 4 (declarative) |
# MAGIC |---|---|---|
# MAGIC | Trigger | We called `writeStream...trigger(availableNow=True)` | SDP picks the trigger from pipeline config |
# MAGIC | Checkpoint | Manual UC volume path | SDP-managed, invisible to us |
# MAGIC | Table creation | We called `.toTable(...)` | `@dp.table` decorator handles it |
# MAGIC | Schema location | Manual UC volume path | SDP-managed |
# MAGIC | Schema hints | Same — passed via reader options | Same |
# MAGIC | Dependency wiring | We ran cells in order | SDP infers the DAG from references |
# MAGIC
# MAGIC ## References
# MAGIC - [Lakeflow SDP Python language reference](https://docs.databricks.com/aws/en/ldp/developer/python-ref)
# MAGIC - [Load data with Auto Loader in pipelines](https://docs.databricks.com/aws/en/ldp/load/auto-loader)
# MAGIC - [What happened to @dlt?](https://docs.databricks.com/aws/en/ldp/where-is-dlt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports
# MAGIC
# MAGIC `pyspark.pipelines` is the new SDP module (replacement for the legacy `dlt` module).
# MAGIC The `dp` alias mirrors the convention from the Databricks migration guide.
# MAGIC
# MAGIC The `pipelines` module is *only* importable inside a pipeline run — it will fail
# MAGIC with `ModuleNotFoundError` if you try to run this notebook attached to a regular
# MAGIC interactive cluster. That's expected; we'll run it via the SDP runtime.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC `RAW_PATH` matches the Phase 0 / Phase 1 GCS layout. The pipeline's catalog and
# MAGIC target schema are set at pipeline-creation time (4.5), not here — that's why
# MAGIC there's no `CATALOG = ...` constant. Hardcoding it would conflict with the
# MAGIC pipeline-level setting and prevent dev-vs-prod target swapping later.

# COMMAND ----------

RAW_PATH = "gs://gcp-bucket-path/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper function: `bronze_stream_reader`
# MAGIC
# MAGIC Builds an Auto Loader streaming DataFrame for a given source folder. Returns a
# MAGIC DataFrame; the calling `@dp.table`-decorated function just adds metadata columns
# MAGIC and returns it.
# MAGIC
# MAGIC ### Auto Loader options used (same as Phase 1)
# MAGIC
# MAGIC | Option | Value | Why |
# MAGIC |---|---|---|
# MAGIC | `cloudFiles.format` | `csv` | All 9 sources are CSV |
# MAGIC | `cloudFiles.useNotifications` | `false` | Phase 0 didn't grant Pub/Sub permissions; directory listing mode works fine for our scale |
# MAGIC | `cloudFiles.inferColumnTypes` | `true` | Same as Phase 1 — let inference handle most columns |
# MAGIC | `cloudFiles.schemaEvolutionMode` | `addNewColumns` | Same as Phase 1 — auto-add new columns if they appear |
# MAGIC | `header` / `multiLine` / `escape` | `true` / `true` / `"` | Reviews CSV has embedded newlines and quotes |
# MAGIC | `cloudFiles.schemaHints` | per-table | Forces specific types where inference is unreliable |
# MAGIC
# MAGIC We do **not** pass `cloudFiles.schemaLocation` — SDP manages that itself.
# MAGIC
# MAGIC The function is intentionally short. Per SDP rules, dataset-defining code must be
# MAGIC deterministic on re-evaluation. This helper just builds a DataFrame description
# MAGIC and returns it; it has no side effects.

# COMMAND ----------

def bronze_stream_reader(source_folder: str, schema_hints: str = None):
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
    )
    if schema_hints:
        reader = reader.option("cloudFiles.schemaHints", schema_hints)
    return reader.load(f"{RAW_PATH}/{source_folder}/")


def add_lineage(df):
    """Add the same three lineage columns we used in Phase 1's Bronze layer."""
    return df.selectExpr(
        "*",
        "_metadata.file_path AS _source_file",
        "_metadata.file_modification_time AS _file_modified_at",
        "current_timestamp() AS _ingested_at",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming tables: one per source folder
# MAGIC
# MAGIC Each function below is decorated with `@dp.table(...)`, which tells SDP:
# MAGIC "this function returns the DataFrame that defines a streaming table." The
# MAGIC table name is taken from the decorator's `name` kwarg (not the function name).
# MAGIC
# MAGIC The `_dlt` suffix on table names is a deliberate convention so we can put SDP
# MAGIC outputs alongside Phase 1–3 tables in Catalog Explorer without collision. The
# MAGIC schema (`dlt_output`) is set at pipeline level.
# MAGIC
# MAGIC ### `table_properties`
# MAGIC
# MAGIC `quality = 'bronze'` matches the convention from Phase 2 (`quality = 'silver'`)
# MAGIC and Phase 3 (`quality = 'gold'`). Discoverable via `DESCRIBE EXTENDED`.
# MAGIC
# MAGIC ### Schema hints — kept identical to Phase 1
# MAGIC
# MAGIC We deliberately do not change schema hints between Phase 1 and Phase 4. The
# MAGIC declarative rebuild should produce equivalent Bronze data so we can prove the
# MAGIC two pipelines agree.

# COMMAND ----------

@dp.table(
    name="bronze_orders_dlt",
    comment="Raw orders ingested via Auto Loader. Mirrors olist_lakehouse_us.bronze.orders.",
    table_properties={"quality": "bronze"},
)
def bronze_orders_dlt():
    return add_lineage(
        bronze_stream_reader(
            "orders",
            schema_hints=(
                "order_purchase_timestamp TIMESTAMP, "
                "order_approved_at TIMESTAMP, "
                "order_delivered_carrier_date TIMESTAMP, "
                "order_delivered_customer_date TIMESTAMP, "
                "order_estimated_delivery_date TIMESTAMP"
            ),
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_order_items_dlt",
    comment="Raw order line items ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_order_items_dlt():
    return add_lineage(
        bronze_stream_reader(
            "order_items",
            schema_hints=(
                "shipping_limit_date TIMESTAMP, "
                "price DOUBLE, "
                "freight_value DOUBLE"
            ),
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_payments_dlt",
    comment="Raw payment records ingested via Auto Loader. Source files are payments_batch1.csv + payments_batch2.csv per Phase 1 demo.",
    table_properties={"quality": "bronze"},
)
def bronze_payments_dlt():
    return add_lineage(
        bronze_stream_reader(
            "payments",
            schema_hints=(
                "payment_sequential INT, "
                "payment_installments INT, "
                "payment_value DOUBLE"
            ),
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_reviews_dlt",
    comment="Raw reviews ingested via Auto Loader. Portuguese free-text fields preserved as-is.",
    table_properties={"quality": "bronze"},
)
def bronze_reviews_dlt():
    return add_lineage(
        bronze_stream_reader(
            "reviews",
            schema_hints=(
                "review_score INT, "
                "review_creation_date TIMESTAMP, "
                "review_answer_timestamp TIMESTAMP"
            ),
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_products_dlt",
    comment="Raw products ingested via Auto Loader. Source typos preserved (lenght → length renamed in Silver).",
    table_properties={"quality": "bronze"},
)
def bronze_products_dlt():
    return add_lineage(
        bronze_stream_reader(
            "products",
            schema_hints=(
                "product_name_lenght INT, "
                "product_description_lenght INT, "
                "product_photos_qty INT, "
                "product_weight_g DOUBLE, "
                "product_length_cm DOUBLE, "
                "product_height_cm DOUBLE, "
                "product_width_cm DOUBLE"
            ),
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_customers_dlt",
    comment="Raw customers ingested via Auto Loader. zip_code_prefix kept as STRING to preserve leading zeros.",
    table_properties={"quality": "bronze"},
)
def bronze_customers_dlt():
    return add_lineage(
        bronze_stream_reader(
            "customers",
            schema_hints="customer_zip_code_prefix STRING",
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_sellers_dlt",
    comment="Raw sellers ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_sellers_dlt():
    return add_lineage(
        bronze_stream_reader(
            "sellers",
            schema_hints="seller_zip_code_prefix STRING",
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_geolocation_dlt",
    comment="Raw geolocation points ingested via Auto Loader. Aggregated to centroids in Silver.",
    table_properties={"quality": "bronze"},
)
def bronze_geolocation_dlt():
    return add_lineage(
        bronze_stream_reader(
            "geolocation",
            schema_hints=(
                "geolocation_zip_code_prefix STRING, "
                "geolocation_lat DOUBLE, "
                "geolocation_lng DOUBLE"
            ),
        )
    )

# COMMAND ----------

@dp.table(
    name="bronze_category_translation_dlt",
    comment="Raw Portuguese-to-English category translation map.",
    table_properties={"quality": "bronze"},
)
def bronze_category_translation_dlt():
    return add_lineage(bronze_stream_reader("category_translation"))