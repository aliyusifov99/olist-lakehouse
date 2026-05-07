# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Phase 6.5 — Governance Audit
# MAGIC
# MAGIC Single notebook, runnable end-to-end, verifies that every governance
# MAGIC artifact applied in 6.1-6.4 is in place and consistent.
# MAGIC
# MAGIC Five check categories:
# MAGIC 1. Table-level comments present on all managed tables
# MAGIC 2. Column-level comments persisted (didn't drift after rebuilds)
# MAGIC 3. All 5 tags applied to every managed table
# MAGIC 4. Tag values fall within the allowed vocabulary
# MAGIC 5. Phase 6.4 access-control objects are registered and attached
# MAGIC
# MAGIC Final cell summarizes PASS/FAIL counts. Re-runnable; no side effects.
# MAGIC
# MAGIC Reference: UC information_schema
# MAGIC https://docs.databricks.com/aws/en/sql/language-manual/information-schema/information-schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check 1: every managed table in bronze/silver/gold has a non-empty,
# MAGIC -- non-trivial comment (length >= 30 chars). The threshold catches
# MAGIC -- placeholder comments like "TODO" or "table".
# MAGIC
# MAGIC SELECT
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   CASE
# MAGIC     WHEN comment IS NULL OR LENGTH(TRIM(comment)) = 0 THEN '❌ MISSING'
# MAGIC     WHEN LENGTH(comment) < 30 THEN '⚠️ TOO SHORT'
# MAGIC     ELSE '✅ OK'
# MAGIC   END AS status,
# MAGIC   LENGTH(comment) AS comment_length
# MAGIC FROM olist_lakehouse_us.information_schema.tables
# MAGIC WHERE table_schema IN ('bronze', 'silver', 'gold')
# MAGIC   AND table_type = 'MANAGED'
# MAGIC ORDER BY status DESC, table_schema, table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check 2: the columns you commented still have comments.
# MAGIC WITH expected_commented_columns AS (
# MAGIC   SELECT * FROM (VALUES
# MAGIC     ('silver', 'customers', 'customer_id'),
# MAGIC     ('silver', 'customers', 'customer_unique_id'),
# MAGIC     ('silver', 'orders', 'order_status'),
# MAGIC     ('silver', 'orders', 'order_purchase_ts'),
# MAGIC     ('silver', 'order_items', 'total_item_value'),
# MAGIC     ('silver', 'order_items', 'price'),
# MAGIC     ('silver', 'order_items', 'freight_value'),
# MAGIC     ('silver', 'payments', 'payment_type_known'),
# MAGIC     ('silver', 'payments', 'installment_bucket'),
# MAGIC     ('silver', 'payments', 'payment_value'),
# MAGIC     ('silver', 'reviews', 'review_id'),
# MAGIC     ('silver', 'reviews', 'sentiment'),
# MAGIC     ('silver', 'products', 'category_name_en'),
# MAGIC     ('silver', 'geolocation', 'lat_centroid'),
# MAGIC     ('silver', 'geolocation', 'source_point_count'),
# MAGIC     ('gold', 'monthly_revenue', 'total_revenue'),
# MAGIC     ('gold', 'monthly_revenue', 'order_count'),
# MAGIC     ('gold', 'monthly_revenue', 'month_start'),
# MAGIC     ('gold', 'customer_rfm', 'customer_unique_id'),
# MAGIC     ('gold', 'customer_rfm', 'monetary'),
# MAGIC     ('gold', 'customer_rfm', 'frequency'),
# MAGIC     ('gold', 'customer_rfm', 'is_repeat_customer'),
# MAGIC     ('gold', 'delivery_performance', 'avg_delay_days'),
# MAGIC     ('gold', 'delivery_performance', 'strict_late_rate_pct'),
# MAGIC     ('gold', 'delivery_performance', 'sla_late_rate_pct'),
# MAGIC     ('gold', 'seller_scorecard', 'composite_score'),
# MAGIC     ('gold', 'geographic_metrics', 'customer_state'),
# MAGIC     ('gold', 'geographic_metrics', 'seller_state'),
# MAGIC     ('gold', 'geographic_metrics', 'total_revenue'),
# MAGIC     ('gold', 'review_trends', 'late_delivery_pct_for_reviewed_orders')
# MAGIC   ) AS t(table_schema, table_name, column_name)
# MAGIC )
# MAGIC SELECT
# MAGIC   e.table_schema,
# MAGIC   e.table_name,
# MAGIC   e.column_name,
# MAGIC   CASE
# MAGIC     WHEN c.comment IS NULL OR LENGTH(TRIM(c.comment)) = 0 THEN '❌ COMMENT MISSING'
# MAGIC     ELSE '✅ OK'
# MAGIC   END AS status,
# MAGIC   LENGTH(c.comment) AS comment_length
# MAGIC FROM expected_commented_columns e
# MAGIC LEFT JOIN olist_lakehouse_us.information_schema.columns c
# MAGIC   ON c.table_schema = e.table_schema
# MAGIC   AND c.table_name = e.table_name
# MAGIC   AND c.column_name = e.column_name
# MAGIC ORDER BY status DESC, e.table_schema, e.table_name, e.column_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check 3: every managed table in bronze/silver/gold has all 5 tags
# MAGIC
# MAGIC WITH expected_tag_keys AS (
# MAGIC   SELECT explode(array(
# MAGIC     'medallion_layer', 'domain', 'pii',
# MAGIC     'refresh_frequency', 'data_classification'
# MAGIC   )) AS tag_name
# MAGIC ),
# MAGIC all_tables AS (
# MAGIC   SELECT table_schema, table_name
# MAGIC   FROM olist_lakehouse_us.information_schema.tables
# MAGIC   WHERE table_schema IN ('bronze', 'silver', 'gold')
# MAGIC     AND table_type = 'MANAGED'
# MAGIC ),
# MAGIC expected AS (
# MAGIC   SELECT t.table_schema, t.table_name, k.tag_name
# MAGIC   FROM all_tables t CROSS JOIN expected_tag_keys k
# MAGIC ),
# MAGIC actual AS (
# MAGIC   SELECT schema_name AS table_schema, table_name, tag_name, tag_value
# MAGIC   FROM olist_lakehouse_us.information_schema.table_tags
# MAGIC   WHERE schema_name IN ('bronze', 'silver', 'gold')
# MAGIC )
# MAGIC SELECT
# MAGIC   e.table_schema,
# MAGIC   e.table_name,
# MAGIC   e.tag_name,
# MAGIC   CASE
# MAGIC     WHEN a.tag_value IS NULL THEN '❌ TAG MISSING'
# MAGIC     ELSE '✅ OK'
# MAGIC   END AS status,
# MAGIC   a.tag_value
# MAGIC FROM expected e
# MAGIC LEFT JOIN actual a USING (table_schema, table_name, tag_name)
# MAGIC ORDER BY status DESC, e.table_schema, e.table_name, e.tag_name;

# COMMAND ----------

