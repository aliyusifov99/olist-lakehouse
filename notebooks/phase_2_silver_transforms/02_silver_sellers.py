# Databricks notebook source
# MAGIC %md
# MAGIC # `02_silver_sellers`
# MAGIC
# MAGIC **Silver Layer:** sellers with zip-code and city normalization.
# MAGIC
# MAGIC - **Source:** `olist_lakehouse_us.bronze.sellers` (3,095 rows)
# MAGIC - **Target:** `olist_lakehouse_us.silver.sellers`
# MAGIC
# MAGIC ## Transformations applied
# MAGIC
# MAGIC 1. Zip-code padding to preserve leading zeros (`LPAD` to 5 chars).
# MAGIC 2. City-name cleanup: `TRIM`, collapse internal multi-spaces, force lowercase.
# MAGIC 3. Add `_processed_at` lineage column.
# MAGIC 4. Enforce `seller_id NOT NULL` (Silver PK contract).
# MAGIC |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.silver.sellers
# MAGIC USING DELTA
# MAGIC COMMENT 'Sellers with normalized zip codes and city names. PK is seller_id.'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'silver',
# MAGIC   'medallion.layer' = 'silver'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   seller_id,
# MAGIC   LPAD(CAST(seller_zip_code_prefix AS STRING), 5, '0')   AS seller_zip_code_prefix,
# MAGIC   LOWER(REGEXP_REPLACE(TRIM(seller_city), '\\s+', ' '))  AS seller_city,
# MAGIC   seller_state,
# MAGIC   _ingested_at,
# MAGIC   CURRENT_TIMESTAMP() AS _processed_at
# MAGIC FROM olist_lakehouse_us.bronze.sellers
# MAGIC WHERE seller_id IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Two checks:
# MAGIC
# MAGIC 1. **Structural** — row count, key uniqueness, null patterns, state count.
# MAGIC 2. **Geographic distribution** — confirms the SP-dominance noted in Phase 1's data quality assessment.
# MAGIC
# MAGIC ### Expected values
# MAGIC
# MAGIC | Metric | Expected |
# MAGIC |---|---|
# MAGIC | `total_rows` | 3,095 |
# MAGIC | `distinct_seller_ids` | 3,095 (PK is unique) |
# MAGIC | `distinct_states` | ≤27 |
# MAGIC | `null_zip` / `null_city` / `null_state` | 0 |
# MAGIC | Top state | SP (likely 60%+) |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                         AS total_rows,
# MAGIC   COUNT(DISTINCT seller_id)                        AS distinct_seller_ids,
# MAGIC   COUNT(DISTINCT seller_state)                     AS distinct_states,
# MAGIC   COUNT(DISTINCT seller_city)                      AS distinct_cities,
# MAGIC   SUM(CASE WHEN seller_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip,
# MAGIC   SUM(CASE WHEN seller_city IS NULL OR seller_city = ''
# MAGIC            THEN 1 ELSE 0 END)                      AS null_or_blank_city,
# MAGIC   SUM(CASE WHEN seller_state IS NULL THEN 1 ELSE 0 END) AS null_state
# MAGIC FROM olist_lakehouse_us.silver.sellers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   seller_state,
# MAGIC   COUNT(*)                                          AS seller_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
# MAGIC FROM olist_lakehouse_us.silver.sellers
# MAGIC GROUP BY seller_state
# MAGIC ORDER BY seller_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

