# Databricks notebook source
# MAGIC %md
# MAGIC # `02_silver_order_items`
# MAGIC
# MAGIC **Silver Layer:** cleaned line items with derived `total_item_value`.
# MAGIC
# MAGIC - **Source:** `olist_lakehouse_us.bronze.order_items` (112,650 rows)
# MAGIC - **Target:** `olist_lakehouse_us.silver.order_items`
# MAGIC
# MAGIC ## Transformations applied
# MAGIC
# MAGIC 1. Cast `shipping_limit_date` to `TIMESTAMP_NTZ` (Brazil-local naive convention).
# MAGIC 2. Defensive type re-assertion on `price` and `freight_value` (DOUBLE).
# MAGIC 3. Derive `total_item_value = price + freight_value` (row-level arithmetic, allowed in Silver).
# MAGIC 4. Add `_processed_at` lineage column.
# MAGIC 5. Enforce composite primary key `(order_id, order_item_id)` — neither column nullable.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.silver.order_items
# MAGIC USING DELTA
# MAGIC COMMENT 'Cleaned order line items with derived total_item_value. '
# MAGIC         'PK is composite (order_id, order_item_id). product_id and seller_id '
# MAGIC         'are FKs to silver.products and silver.sellers respectively. '
# MAGIC         'shipping_limit_date is naive Brazil local time (America/Sao_Paulo).'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'silver',
# MAGIC   'medallion.layer' = 'silver',
# MAGIC   'source.timezone' = 'America/Sao_Paulo'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- Composite PK
# MAGIC   order_id,
# MAGIC   order_item_id,
# MAGIC
# MAGIC   -- Foreign keys
# MAGIC   product_id,
# MAGIC   seller_id,
# MAGIC
# MAGIC   -- Cleaned timestamp
# MAGIC   CAST(shipping_limit_date AS TIMESTAMP_NTZ) AS shipping_limit_ts,
# MAGIC
# MAGIC   -- Defensive type re-assertion (Bronze schema hint already typed these as DOUBLE)
# MAGIC   CAST(price         AS DOUBLE) AS price,
# MAGIC   CAST(freight_value AS DOUBLE) AS freight_value,
# MAGIC
# MAGIC   -- Derived: full line-item value
# MAGIC   ROUND(CAST(price AS DOUBLE) + CAST(freight_value AS DOUBLE), 2) AS total_item_value,
# MAGIC
# MAGIC   -- Lineage
# MAGIC   _ingested_at,
# MAGIC   CURRENT_TIMESTAMP() AS _processed_at
# MAGIC
# MAGIC FROM olist_lakehouse_us.bronze.order_items
# MAGIC WHERE order_id IS NOT NULL          -- Silver contract: no null PKs
# MAGIC   AND order_item_id IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Three checks:
# MAGIC
# MAGIC 1. **Structural** — row counts, key uniqueness, items-per-order distribution.
# MAGIC 2. **Business rules** — assertions that prices are positive and freight is non-negative.
# MAGIC 3. **Aggregate sanity** — total revenue figure to spot-check against the project plan's "~$16M BRL over 2 years."
# MAGIC
# MAGIC ### Expected values
# MAGIC
# MAGIC | Metric | Expected |
# MAGIC |---|---|
# MAGIC | `total_rows` | 112,650 |
# MAGIC | `distinct_pk_pairs` | 112,650 (composite key is unique) |
# MAGIC | `distinct_orders` | 98,666 (= 112,650 − 13,984 multi-item rows) |
# MAGIC | `distinct_products` | ~32,951 or fewer (some products may not have sold) |
# MAGIC | `distinct_sellers` | ~3,095 or fewer |
# MAGIC | `negative_prices` | 0 |
# MAGIC | `negative_freight` | 0 |
# MAGIC | `null_total_value` | 0 |
# MAGIC | `total_revenue_brl` | ~16,000,000 |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                       AS total_rows,
# MAGIC   COUNT(DISTINCT (order_id, order_item_id))      AS distinct_pk_pairs,
# MAGIC   COUNT(DISTINCT order_id)                       AS distinct_orders,
# MAGIC   COUNT(DISTINCT product_id)                     AS distinct_products,
# MAGIC   COUNT(DISTINCT seller_id)                      AS distinct_sellers,
# MAGIC   ROUND(AVG(price), 2)                           AS avg_price,
# MAGIC   ROUND(AVG(freight_value), 2)                   AS avg_freight,
# MAGIC   ROUND(SUM(total_item_value), 2)                AS total_revenue_brl
# MAGIC FROM olist_lakehouse_us.silver.order_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Each row should be 0. Any violation surfaces as a non-zero count to investigate.
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN price < 0           THEN 1 ELSE 0 END) AS negative_prices,
# MAGIC   SUM(CASE WHEN price = 0           THEN 1 ELSE 0 END) AS zero_prices,
# MAGIC   SUM(CASE WHEN freight_value < 0   THEN 1 ELSE 0 END) AS negative_freight,
# MAGIC   SUM(CASE WHEN total_item_value IS NULL THEN 1 ELSE 0 END) AS null_total_value,
# MAGIC   SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
# MAGIC   SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id
# MAGIC FROM olist_lakehouse_us.silver.order_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity-check that we have realistic multi-item orders. Phase 1 said "up to 21 items per order".
# MAGIC SELECT
# MAGIC   items_in_order,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
# MAGIC FROM (
# MAGIC   SELECT order_id, COUNT(*) AS items_in_order
# MAGIC   FROM olist_lakehouse_us.silver.order_items
# MAGIC   GROUP BY order_id
# MAGIC )
# MAGIC GROUP BY items_in_order
# MAGIC ORDER BY items_in_order;

# COMMAND ----------

