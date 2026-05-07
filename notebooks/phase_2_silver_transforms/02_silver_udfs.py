# Databricks notebook source
# MAGIC %md
# MAGIC # `02_silver_udfs`
# MAGIC
# MAGIC Unity Catalog SQL UDFs for review sentiment + delivery SLA.
# MAGIC
# MAGIC These are **SQL UDFs** (not Python UDFs) for three reasons:
# MAGIC
# MAGIC 1. The logic is pure `CASE WHEN` — no Python primitives needed.
# MAGIC 2. SQL UDFs run inside Photon; Python UDFs disable Photon for that stage.
# MAGIC 3. Persisted in Unity Catalog — governed, discoverable, reusable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## UDF 1: `classify_review_sentiment`
# MAGIC
# MAGIC Maps `(review_score, comment_length)` → sentiment bucket. Used by `silver.reviews`
# MAGIC to enrich raw 1–5 scores with an engagement signal.
# MAGIC
# MAGIC | Score | Comment | → Sentiment |
# MAGIC | --- | --- | --- |
# MAGIC | NULL | — | `unknown` |
# MAGIC | 1–2 | any | `negative` |
# MAGIC | 3 | length > 50 | `mixed_negative` |
# MAGIC | 3 | else | `neutral` |
# MAGIC | 4 | any | `positive` |
# MAGIC | 5 | length > 0 | `promoter` |
# MAGIC | 5 | else | `positive` |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION olist_lakehouse_us.silver.classify_review_sentiment(
# MAGIC   review_score   INT     COMMENT 'Review score, 1-5',
# MAGIC   comment_length INT     COMMENT 'Length of the review comment in chars; 0 if no comment'
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Classify a review into a sentiment bucket using score and comment engagement. '
# MAGIC         'Returns one of: negative, mixed_negative, neutral, positive, promoter, unknown.'
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN review_score IS NULL THEN 'unknown'
# MAGIC     WHEN review_score <= 2 THEN 'negative'
# MAGIC     WHEN review_score = 3 AND COALESCE(comment_length, 0) > 50 THEN 'mixed_negative'
# MAGIC     WHEN review_score = 3 THEN 'neutral'
# MAGIC     WHEN review_score = 4 THEN 'positive'
# MAGIC     WHEN review_score = 5 AND COALESCE(comment_length, 0) > 0 THEN 'promoter'
# MAGIC     WHEN review_score = 5 THEN 'positive'
# MAGIC     ELSE 'unknown'
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## UDF 2: `delivery_sla_status`
# MAGIC
# MAGIC Maps `delivery_delay_days` → SLA bucket. Input is the *difference*
# MAGIC `(actual − estimated)`, which `silver.orders` already computes. `NULL` input
# MAGIC means the order wasn't delivered.
# MAGIC
# MAGIC | Delay (days) | → SLA bucket |
# MAGIC | --- | --- |
# MAGIC | NULL | `not_delivered` |
# MAGIC | ≤ −3 | `early` |
# MAGIC | ≤ 2 | `on_time` |
# MAGIC | ≤ 7 | `slightly_late` |
# MAGIC | > 7 | `very_late` |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION olist_lakehouse_us.silver.delivery_sla_status(
# MAGIC   delivery_delay_days INT COMMENT 'Days late vs. estimated delivery date. '
# MAGIC                                   'Negative = early. NULL = not delivered.'
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Bucket delivery performance against the SLA estimate. Returns one of: '
# MAGIC         'early, on_time, slightly_late, very_late, not_delivered.'
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN delivery_delay_days IS NULL THEN 'not_delivered'
# MAGIC     WHEN delivery_delay_days <= -3 THEN 'early'
# MAGIC     WHEN delivery_delay_days <= 2  THEN 'on_time'
# MAGIC     WHEN delivery_delay_days <= 7  THEN 'slightly_late'
# MAGIC     ELSE 'very_late'
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## SLA bucket distribution across all orders
# MAGIC
# MAGIC `ORDER BY` references the `SELECT`-list alias `sla_bucket` (which is in scope
# MAGIC post-aggregation), not the source column `delivery_delay_days` (which isn't).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   olist_lakehouse_us.silver.delivery_sla_status(delivery_delay_days) AS sla_bucket,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
# MAGIC FROM olist_lakehouse_us.silver.orders
# MAGIC GROUP BY 1
# MAGIC ORDER BY
# MAGIC   CASE sla_bucket
# MAGIC     WHEN 'early'         THEN 1
# MAGIC     WHEN 'on_time'       THEN 2
# MAGIC     WHEN 'slightly_late' THEN 3
# MAGIC     WHEN 'very_late'     THEN 4
# MAGIC     WHEN 'not_delivered' THEN 5
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot-check the sentiment UDF
# MAGIC
# MAGIC Uses `bronze.reviews` (since `silver.reviews` is built next). Computes
# MAGIC `comment_length` inline, then feeds it to the UDF.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   olist_lakehouse_us.silver.classify_review_sentiment(
# MAGIC     review_score,
# MAGIC     CASE
# MAGIC       WHEN review_comment_message IS NULL THEN 0
# MAGIC       ELSE LENGTH(TRIM(review_comment_message))
# MAGIC     END
# MAGIC   ) AS sentiment,
# MAGIC   COUNT(*) AS review_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
# MAGIC FROM olist_lakehouse_us.bronze.reviews
# MAGIC GROUP BY 1
# MAGIC ORDER BY review_count DESC;

# COMMAND ----------

