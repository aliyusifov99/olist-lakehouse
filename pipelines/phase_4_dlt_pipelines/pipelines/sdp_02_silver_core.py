# Databricks notebook source
# MAGIC %md
# MAGIC # SDP Silver Layer — Core Entities
# MAGIC
# MAGIC Recreates the first 5 of Phase 2's Silver tables as SDP datasets:
# MAGIC `orders`, `order_items`, `customers`, `products`, `sellers`. The remaining 3
# MAGIC (`payments`, `reviews`, `geolocation`) are in `sdp_02_silver_enrich`.
# MAGIC
# MAGIC ## Patterns demonstrated in this notebook
# MAGIC
# MAGIC | Pattern | Where |
# MAGIC |---|---|
# MAGIC | Streaming table reading from Bronze streaming table | All 5 tables |
# MAGIC | Stream-static join (streaming Bronze + static lookup) | `silver_products_dlt` |
# MAGIC | `@dp.expect_all` (batch expectations) | `silver_orders_dlt`, `silver_order_items_dlt` |
# MAGIC | `@dp.expect_or_drop` for noise filtering | `silver_orders_dlt` |
# MAGIC | `@dp.expect_or_fail` for hard invariants | All 5 tables (PK non-null) |
# MAGIC | UDF reuse from Unity Catalog | `silver_orders_dlt` (delivery_sla_status) |
# MAGIC | TIMESTAMP_NTZ for naive Brazil-local time | All timestamp columns |
# MAGIC
# MAGIC ## References
# MAGIC - [SDP Python language reference](https://docs.databricks.com/aws/en/ldp/developer/python-ref)
# MAGIC - [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/ldp/expectations)
# MAGIC - [Streaming tables](https://docs.databricks.com/aws/en/ldp/streaming-tables)
# MAGIC - [TIMESTAMP_NTZ](https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-ntz-type)

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, expr, datediff, year, month, quarter, to_date, current_timestamp,
    when, lower, trim, lit, coalesce, round as spark_round
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared TBLPROPERTIES
# MAGIC
# MAGIC Mirrors the Phase 2 convention: every Silver table carries `quality`,
# MAGIC `medallion.layer`, and (for tables with timestamps) `source.timezone`.
# MAGIC Unity Catalog surfaces these in `DESCRIBE EXTENDED` and Catalog Explorer.

# COMMAND ----------

SILVER_PROPS = {
    "quality": "silver",
    "medallion.layer": "silver",
}
SILVER_PROPS_TS = {
    **SILVER_PROPS,
    "source.timezone": "America/Sao_Paulo",
    "delta.feature.timestampNtz": "supported",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver_orders_dlt` — orders with delivery metrics
# MAGIC
# MAGIC Mirrors `silver.orders` from Phase 2. Five timestamp columns cast to
# MAGIC TIMESTAMP_NTZ; four derived metrics (`delivery_days`, `is_late_delivery`,
# MAGIC `delivery_delay_days`, plus the SLA UDF bucket); time dimensions for analytics.
# MAGIC
# MAGIC ### Expectation severity choices
# MAGIC
# MAGIC | Check | Decorator | Why this severity |
# MAGIC |---|---|---|
# MAGIC | `order_id IS NOT NULL` | `expect_or_fail` | Phase 2 audit found 0 NULLs. Any violation = Bronze is broken, stop loud. |
# MAGIC | `customer_id IS NOT NULL` | `expect_or_fail` | Same — required FK to customers. |
# MAGIC | `order_status IN (...)` | `expect_or_drop` | Unknown statuses indicate either source-system change or corrupt rows; safer to drop and surface in event log. |
# MAGIC | `delivery_days >= 0 OR delivery_days IS NULL` | `expect` | Phase 2 found 0 negatives, but treat as warn-only — could legitimately be a Bronze-side data entry anomaly worth investigating without halting. |
# MAGIC
# MAGIC ### UDF call inside SDP
# MAGIC
# MAGIC We reuse `silver.delivery_sla_status` from Phase 2. SDP doesn't restrict UC UDF
# MAGIC calls — they work the same way they do in any other Spark job. We use
# MAGIC `expr("silver.delivery_sla_status(...)")` to invoke the SQL UDF; this keeps
# MAGIC execution in Photon (Python UDFs would disable Photon for that stage).
# MAGIC The fully-qualified name is required because the SDP pipeline's default
# MAGIC schema is `dlt_output`, not `silver`.

# COMMAND ----------

@dp.table(
    name="silver_orders_dlt",
    comment="Cleaned orders with delivery metrics. Streams from bronze_orders_dlt.",
    table_properties=SILVER_PROPS_TS,
)
@dp.expect_or_fail("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop(
    "valid_status",
    "order_status IN ('delivered','shipped','canceled','unavailable',"
    "'invoiced','processing','created','approved')",
)
@dp.expect("delivery_days_non_negative", "delivery_days >= 0 OR delivery_days IS NULL")
def silver_orders_dlt():
    return (
        spark.readStream.table("bronze_orders_dlt")
        .select(
            col("order_id"),
            col("customer_id"),
            col("order_status"),
            col("order_purchase_timestamp").cast("timestamp_ntz").alias("order_purchase_ts"),
            col("order_approved_at").cast("timestamp_ntz").alias("order_approved_ts"),
            col("order_delivered_carrier_date").cast("timestamp_ntz").alias("delivered_to_carrier_ts"),
            col("order_delivered_customer_date").cast("timestamp_ntz").alias("delivered_to_customer_ts"),
            col("order_estimated_delivery_date").cast("timestamp_ntz").alias("estimated_delivery_ts"),
            col("_ingested_at"),
        )
        .withColumn("delivery_days",
            datediff(col("delivered_to_customer_ts"), col("order_purchase_ts")))
        .withColumn("delivery_delay_days",
            datediff(col("delivered_to_customer_ts"), col("estimated_delivery_ts")))
        .withColumn("is_late_delivery",
            when(col("delivered_to_customer_ts").isNull(), None)
            .when(col("delivered_to_customer_ts") > col("estimated_delivery_ts"), True)
            .otherwise(False))
        .withColumn("sla_status",
            expr("olist_lakehouse_us.silver.delivery_sla_status(delivery_delay_days)"))
        .withColumn("order_year", year(col("order_purchase_ts")))
        .withColumn("order_month", month(col("order_purchase_ts")))
        .withColumn("order_quarter", quarter(col("order_purchase_ts")))
        .withColumn("order_date", to_date(col("order_purchase_ts")))
        .withColumn("_processed_at", current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver_order_items_dlt` — line items with `total_item_value`
# MAGIC
# MAGIC Composite PK is `(order_id, order_item_id)` — same as Phase 2. We don't
# MAGIC enforce the composite uniqueness as an expectation because evaluating it
# MAGIC requires aggregation across rows, which doesn't fit the per-row predicate
# MAGIC model. PK uniqueness is verified post-run in comparison notebook.
# MAGIC
# MAGIC ### `expect_all` batch syntax
# MAGIC
# MAGIC When you have multiple `expect_or_fail` checks on the same dataset,
# MAGIC `expect_all_or_fail` (and the `_drop` and warn-only variants) are cleaner —
# MAGIC pass a dict of `{name: predicate}`. Behaviorally identical to stacking
# MAGIC individual decorators.

# COMMAND ----------

@dp.table(
    name="silver_order_items_dlt",
    comment="Line items with total_item_value = price + freight_value. Streams from bronze_order_items_dlt.",
    table_properties=SILVER_PROPS_TS,
)
@dp.expect_all_or_fail({
    "valid_order_id": "order_id IS NOT NULL",
    "valid_order_item_id": "order_item_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "valid_seller_id": "seller_id IS NOT NULL",
})
@dp.expect_all({
    "non_negative_price": "price >= 0",
    "non_negative_freight": "freight_value >= 0",
})
def silver_order_items_dlt():
    return (
        spark.readStream.table("bronze_order_items_dlt")
        .select(
            col("order_id"),
            col("order_item_id").cast("int").alias("order_item_id"),
            col("product_id"),
            col("seller_id"),
            col("shipping_limit_date").cast("timestamp_ntz").alias("shipping_limit_ts"),
            col("price").cast("double").alias("price"),
            col("freight_value").cast("double").alias("freight_value"),
            col("_ingested_at"),
        )
        .withColumn("total_item_value", spark_round(col("price") + col("freight_value"), 2))
        .withColumn("_processed_at", current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver_customers_dlt` — customers at per-order grain
# MAGIC
# MAGIC Per Phase 2's decision: customers stay at the per-order `customer_id` grain,
# MAGIC NOT collapsed to `customer_unique_id`. The Gold layer collapses for RFM.
# MAGIC City names are normalized (trim + lowercase) the same as Phase 2.

# COMMAND ----------

@dp.table(
    name="silver_customers_dlt",
    comment="Customers at per-order grain. customer_unique_id used for RFM in Gold.",
    table_properties=SILVER_PROPS,
)
@dp.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_fail("valid_customer_unique_id", "customer_unique_id IS NOT NULL")
@dp.expect("valid_state_code", "length(customer_state) = 2")
def silver_customers_dlt():
    return (
        spark.readStream.table("bronze_customers_dlt")
        .select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_zip_code_prefix"),
            trim(lower(col("customer_city"))).alias("customer_city"),
            col("customer_state"),
            col("_ingested_at"),
        )
        .withColumn("_processed_at", current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver_sellers_dlt`
# MAGIC
# MAGIC 3,095 rows total. Same normalization pattern as customers.

# COMMAND ----------

@dp.table(
    name="silver_sellers_dlt",
    comment="Sellers with normalized city names.",
    table_properties=SILVER_PROPS,
)
@dp.expect_or_fail("valid_seller_id", "seller_id IS NOT NULL")
@dp.expect("valid_state_code", "length(seller_state) = 2")
def silver_sellers_dlt():
    return (
        spark.readStream.table("bronze_sellers_dlt")
        .select(
            col("seller_id"),
            col("seller_zip_code_prefix"),
            trim(lower(col("seller_city"))).alias("seller_city"),
            col("seller_state"),
            col("_ingested_at"),
        )
        .withColumn("_processed_at", current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver_products_dlt` — products with English category translation
# MAGIC
# MAGIC ### Stream-static join pattern
# MAGIC
# MAGIC The Bronze products feed (`bronze_products_dlt`) is read as a stream.
# MAGIC The category translation (`bronze_category_translation_dlt`) is read as
# MAGIC a static DataFrame via `spark.read.table(...)`. This is a "stream-static
# MAGIC join" — Spark reloads the static side for each microbatch, which is fine
# MAGIC for small lookups (74 rows here).
# MAGIC
# MAGIC SDP infers both dependencies from the references. The translation table
# MAGIC will appear as an upstream node in the pipeline DAG even though it's used
# MAGIC as a static lookup.

# COMMAND ----------

@dp.table(
    name="silver_products_dlt",
    comment=(
        "Products with English category names and product_volume_cm3. "
        "Stream-static join with bronze_category_translation_dlt."
    ),
    table_properties=SILVER_PROPS,
)
@dp.expect_or_fail("valid_product_id", "product_id IS NOT NULL")
@dp.expect("category_resolved", "category_name_en IS NOT NULL")
def silver_products_dlt():
    products = spark.readStream.table("bronze_products_dlt")
    translation = spark.read.table("bronze_category_translation_dlt")

    return (
        products.alias("p")
        .join(
            translation.alias("t"),
            col("p.product_category_name") == col("t.product_category_name"),
            "left",
        )
        .select(
            col("p.product_id").alias("product_id"),
            col("p.product_category_name").alias("category_name_pt"),
            coalesce(
                col("t.product_category_name_english"),
                when(col("p.product_category_name") == "pc_gamer", lit("pc_gaming"))
                .when(
                    col("p.product_category_name") == "portateis_cozinha_e_preparadores_de_alimentos",
                    lit("portable_kitchen_food_processors"),
                )
                .otherwise(lit("unknown")),
            ).alias("category_name_en"),
            col("p.product_weight_g").cast("double").alias("product_weight_g"),
            col("p.product_length_cm").cast("double").alias("product_length_cm"),
            col("p.product_height_cm").cast("double").alias("product_height_cm"),
            col("p.product_width_cm").cast("double").alias("product_width_cm"),
            col("p.product_name_lenght").alias("product_name_length"),
            col("p.product_description_lenght").alias("product_description_length"),
            col("p.product_photos_qty").alias("product_photos_qty"),
            col("p._ingested_at").alias("_ingested_at"),
        )
        .withColumn(
            "product_volume_cm3",
            spark_round(
                col("product_length_cm") * col("product_height_cm") * col("product_width_cm"),
                2,
            ),
        )
        .withColumn("_processed_at", current_timestamp())
    )