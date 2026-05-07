# Databricks notebook source
# MAGIC %md
# MAGIC # `02_silver_customers`
# MAGIC
# MAGIC **Silver Layer:** customers at the per-order grain (one row per `customer_id`).
# MAGIC
# MAGIC - **Source:** `olist_lakehouse_us.bronze.customers` (99,441 rows)
# MAGIC - **Target:** `olist_lakehouse_us.silver.customers`
# MAGIC
# MAGIC ## Transformations applied
# MAGIC
# MAGIC 1. Defensive zip-code padding to preserve leading zeros (`LPAD` to 5 chars).
# MAGIC 2. City-name cleanup: `TRIM`, collapse internal multi-spaces, force lowercase.
# MAGIC 3. Add `_processed_at` lineage column.
# MAGIC 4. Enforce `customer_id NOT NULL` (Silver PK contract).
# MAGIC
# MAGIC ## Why we don't dedupe to `customer_unique_id`
# MAGIC
# MAGIC `customer_id` is **per-order** in Olist's data model — a new `customer_id` is generated every time the same person places an order. The stable identity is `customer_unique_id`. Two reasons we keep the per-order grain in Silver:
# MAGIC
# MAGIC 1. **Join key.** `silver.orders` joins on `customer_id`. Collapsing this table to `customer_unique_id` would break the link from orders → customers.
# MAGIC 2. **Address signal.** A repeat customer ordering to different cities is meaningfully different from one ordering to the same address. Collapsing destroys this.
# MAGIC
# MAGIC RFM segmentation in Gold (Phase 3) aggregates by `customer_unique_id`. That's a Gold concern; Silver preserves the natural grain.
# MAGIC
# MAGIC ## Why minimal city normalization
# MAGIC
# MAGIC `customer_city` is free-text with accent variants ("são paulo" vs "sao paulo") and spelling variants. Full normalization needs accent-folding and a fuzzy-match table — out of scope. We do the cheap pass (whitespace + case) and accept the rest. State-level (`customer_state`) is the analytical aggregation unit anyway, and that's already a clean 2-char code.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.silver.customers
# MAGIC USING DELTA
# MAGIC COMMENT 'Customers at the per-order grain. customer_id is per-order; '
# MAGIC         'customer_unique_id is the stable per-person identity. '
# MAGIC         'PK is customer_id. Join silver.orders on customer_id.'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'silver',
# MAGIC   'medallion.layer' = 'silver'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- PK: per-order customer identity
# MAGIC   customer_id,
# MAGIC
# MAGIC   -- Stable per-person identity (used for RFM in Gold)
# MAGIC   customer_unique_id,
# MAGIC
# MAGIC   -- Zip prefix as 5-char string with preserved leading zeros
# MAGIC   LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0') AS customer_zip_code_prefix,
# MAGIC
# MAGIC   -- City: trim, collapse multi-spaces, force lowercase
# MAGIC   LOWER(REGEXP_REPLACE(TRIM(customer_city), '\\s+', ' ')) AS customer_city,
# MAGIC
# MAGIC   -- State: 2-char code, already clean
# MAGIC   customer_state,
# MAGIC
# MAGIC   -- Lineage
# MAGIC   _ingested_at,
# MAGIC   CURRENT_TIMESTAMP() AS _processed_at
# MAGIC
# MAGIC FROM olist_lakehouse_us.bronze.customers
# MAGIC WHERE customer_id IS NOT NULL;  -- Silver PK contract

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Three checks:
# MAGIC
# MAGIC 1. **Structural** — row count, key uniqueness at both grains, repeat-customer rate.
# MAGIC 2. **State coverage** — should be ≤27 (Brazil has 27 states/federal districts).
# MAGIC 3. **Repeat-customer distribution** — confirms the plan's "~3% repeat rate" finding.
# MAGIC
# MAGIC ### Expected values
# MAGIC
# MAGIC | Metric | Expected |
# MAGIC |---|---|
# MAGIC | `total_rows` | 99,441 |
# MAGIC | `distinct_customer_ids` | 99,441 (PK is unique) |
# MAGIC | `distinct_customer_unique_ids` | ~96,096 |
# MAGIC | `repeat_pct` | ~3% |
# MAGIC | `distinct_states` | ≤27 |
# MAGIC | `null_zip` / `null_city` / `null_state` | 0 |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                      AS total_rows,
# MAGIC   COUNT(DISTINCT customer_id)                   AS distinct_customer_ids,
# MAGIC   COUNT(DISTINCT customer_unique_id)            AS distinct_customer_unique_ids,
# MAGIC   ROUND(
# MAGIC     100.0 * (COUNT(*) - COUNT(DISTINCT customer_unique_id))
# MAGIC           / COUNT(DISTINCT customer_unique_id),
# MAGIC     2
# MAGIC   ) AS repeat_orders_pct,
# MAGIC   COUNT(DISTINCT customer_state)                AS distinct_states,
# MAGIC   SUM(CASE WHEN customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip,
# MAGIC   SUM(CASE WHEN customer_city IS NULL OR customer_city = ''
# MAGIC            THEN 1 ELSE 0 END)                  AS null_or_blank_city,
# MAGIC   SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END) AS null_state
# MAGIC FROM olist_lakehouse_us.silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many orders does each customer place?
# MAGIC -- Confirms the long-tail "most customers buy once" pattern.
# MAGIC SELECT
# MAGIC   orders_per_customer,
# MAGIC   COUNT(*) AS customer_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
# MAGIC FROM (
# MAGIC   SELECT customer_unique_id, COUNT(*) AS orders_per_customer
# MAGIC   FROM olist_lakehouse_us.silver.customers
# MAGIC   GROUP BY customer_unique_id
# MAGIC )
# MAGIC GROUP BY orders_per_customer
# MAGIC ORDER BY orders_per_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Geographic distribution sanity check.
# MAGIC -- SP (Sao Paulo) should dominate; the long tail covers smaller states.
# MAGIC SELECT
# MAGIC   customer_state,
# MAGIC   COUNT(*) AS customer_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
# MAGIC FROM olist_lakehouse_us.silver.customers
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY customer_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

