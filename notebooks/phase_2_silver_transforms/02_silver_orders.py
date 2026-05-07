# Databricks notebook source
# MAGIC %md
# MAGIC # `02_silver_orders`
# MAGIC
# MAGIC **Silver Layer:** cleaned orders with derived delivery metrics.
# MAGIC
# MAGIC - **Source:** `olist_lakehouse_us.bronze.orders` (99,441 rows)
# MAGIC - **Target:** `olist_lakehouse_us.silver.orders`
# MAGIC
# MAGIC ## Transformations applied
# MAGIC
# MAGIC 1. Cast naive Brazil-local timestamps to `TIMESTAMP_NTZ`.
# MAGIC 2. Rename timestamp columns to the `*_ts` convention.
# MAGIC 3. Derive delivery metrics: `delivery_days`, `is_late_delivery`, `delivery_delay_days`.
# MAGIC 4. Derive time dimensions: `order_year`, `order_month`, `order_quarter`, `order_date`.
# MAGIC 5. Add `_processed_at` lineage column.
# MAGIC 6. Enforce `order_id NOT NULL` (Silver contract: no null PKs).
# MAGIC
# MAGIC ## Notes on timestamp handling
# MAGIC
# MAGIC Bronze inferred timestamps as `TIMESTAMP` (timezone-aware) via schema hints, but
# MAGIC the underlying source is naive Brazil local time — `TIMESTAMP_NTZ` is the
# MAGIC honest type, so we re-cast in the `typed` CTE.
# MAGIC
# MAGIC ## Notes on derived columns
# MAGIC
# MAGIC - `is_late_delivery` is **three-valued**: `TRUE` (late), `FALSE` (on time),
# MAGIC   `NULL` (not delivered yet / canceled).
# MAGIC - `delivery_delay_days` is `actual − estimated` — negative means early; `NULL`
# MAGIC   when not delivered.
# MAGIC - Time dimensions are derived from `order_purchase_ts` since that's the
# MAGIC   business "order date".

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.silver.orders
# MAGIC USING DELTA
# MAGIC COMMENT 'Cleaned orders with derived delivery and time-dimension columns. '
# MAGIC         'Timestamps are naive Brazil local time (America/Sao_Paulo).'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'silver',
# MAGIC   'medallion.layer' = 'silver',
# MAGIC   'source.timezone' = 'America/Sao_Paulo'
# MAGIC )
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_status,
# MAGIC     CAST(order_purchase_timestamp       AS TIMESTAMP_NTZ) AS order_purchase_ts,
# MAGIC     CAST(order_approved_at              AS TIMESTAMP_NTZ) AS order_approved_ts,
# MAGIC     CAST(order_delivered_carrier_date   AS TIMESTAMP_NTZ) AS delivered_to_carrier_ts,
# MAGIC     CAST(order_delivered_customer_date  AS TIMESTAMP_NTZ) AS delivered_to_customer_ts,
# MAGIC     CAST(order_estimated_delivery_date  AS TIMESTAMP_NTZ) AS estimated_delivery_ts,
# MAGIC     _ingested_at
# MAGIC   FROM olist_lakehouse_us.bronze.orders
# MAGIC   WHERE order_id IS NOT NULL  -- Silver contract: no null PKs
# MAGIC )
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_status,
# MAGIC
# MAGIC   order_purchase_ts,
# MAGIC   order_approved_ts,
# MAGIC   delivered_to_carrier_ts,
# MAGIC   delivered_to_customer_ts,
# MAGIC   estimated_delivery_ts,
# MAGIC
# MAGIC   -- Delivery duration in days; NULL when not yet delivered
# MAGIC   DATEDIFF(delivered_to_customer_ts, order_purchase_ts) AS delivery_days,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN delivered_to_customer_ts IS NULL THEN NULL
# MAGIC     WHEN delivered_to_customer_ts > estimated_delivery_ts THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_late_delivery,
# MAGIC
# MAGIC   DATEDIFF(delivered_to_customer_ts, estimated_delivery_ts) AS delivery_delay_days,
# MAGIC
# MAGIC   YEAR(order_purchase_ts)               AS order_year,
# MAGIC   MONTH(order_purchase_ts)              AS order_month,
# MAGIC   QUARTER(order_purchase_ts)            AS order_quarter,
# MAGIC   CAST(order_purchase_ts AS DATE)       AS order_date,
# MAGIC
# MAGIC   _ingested_at,
# MAGIC   CURRENT_TIMESTAMP()                   AS _processed_at
# MAGIC FROM typed;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check
# MAGIC
# MAGIC Row counts and null distribution on key derived columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                AS total_rows,
# MAGIC   COUNT(DISTINCT order_id)                                AS distinct_order_ids,
# MAGIC   COUNT(delivered_to_customer_ts)                         AS rows_with_delivery_ts,
# MAGIC   SUM(CASE WHEN is_late_delivery = TRUE  THEN 1 ELSE 0 END) AS late_orders,
# MAGIC   SUM(CASE WHEN is_late_delivery = FALSE THEN 1 ELSE 0 END) AS on_time_orders,
# MAGIC   SUM(CASE WHEN is_late_delivery IS NULL THEN 1 ELSE 0 END) AS undelivered_orders,
# MAGIC   ROUND(AVG(delivery_days), 1)                            AS avg_delivery_days
# MAGIC FROM olist_lakehouse_us.silver.orders;

# COMMAND ----------

