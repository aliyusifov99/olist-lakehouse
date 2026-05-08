# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Phase 6 — Table Tags
# MAGIC
# MAGIC Applies a 5-key tag vocabulary to every Bronze, Silver, and Gold table:
# MAGIC
# MAGIC | Key | Allowed values |
# MAGIC |---|---|
# MAGIC | medallion_layer | bronze / silver / gold |
# MAGIC | domain | finance / marketing / operations / infrastructure |
# MAGIC | pii | true / false |
# MAGIC | refresh_frequency | static / daily / weekly |
# MAGIC | data_classification | public / internal / restricted |
# MAGIC
# MAGIC PII rationale: Olist's public dataset is anonymized, so no table contains
# MAGIC real names/emails. The pii tag marks tables that would carry PII in a
# MAGIC production version of this pipeline (customers, customer_rfm) — so the
# MAGIC governance pattern is in place if real data ever flows through.
# MAGIC
# MAGIC Reference: Databricks UC Tags
# MAGIC https://docs.databricks.com/aws/en/database-objects/tags
# MAGIC
# MAGIC Idempotent: SET TAGS upserts, so re-running updates values in place.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze: source-exact mirrors. Refresh frequency depends on whether
# MAGIC -- the table is transactional (daily) or reference data (static).
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.bronze.orders SET TAGS (
# MAGIC   'medallion_layer' = 'bronze',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.bronze.order_items SET TAGS (
# MAGIC   'medallion_layer' = 'bronze',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.bronze.customers SET TAGS (
# MAGIC   'medallion_layer' = 'bronze',
# MAGIC   'domain' = 'marketing',
# MAGIC   'pii' = 'true',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'restricted'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.bronze.payments SET TAGS (
# MAGIC   'medallion_layer' = 'bronze',
# MAGIC   'domain' = 'finance',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.bronze.reviews SET TAGS (
# MAGIC   'medallion_layer' = 'bronze',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.bronze.products SET TAGS (
# MAGIC   'medallion_layer' = 'bronze',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'static',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.bronze.sellers SET TAGS (
# MAGIC   'medallion_layer' = 'bronze',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'static',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.bronze.geolocation SET TAGS (
# MAGIC   'medallion_layer' = 'bronze',
# MAGIC   'domain' = 'infrastructure',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'static',
# MAGIC   'data_classification' = 'public'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.bronze.category_translation SET TAGS (
# MAGIC   'medallion_layer' = 'bronze',
# MAGIC   'domain' = 'infrastructure',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'static',
# MAGIC   'data_classification' = 'public'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver: same domain assignments as Bronze (one-to-one mirror).
# MAGIC -- pii inherits from Bronze. Geolocation classification stays public —
# MAGIC -- aggregating to centroids doesn't change the classification.
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.orders SET TAGS (
# MAGIC   'medallion_layer' = 'silver',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.order_items SET TAGS (
# MAGIC   'medallion_layer' = 'silver',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.customers SET TAGS (
# MAGIC   'medallion_layer' = 'silver',
# MAGIC   'domain' = 'marketing',
# MAGIC   'pii' = 'true',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'restricted'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.payments SET TAGS (
# MAGIC   'medallion_layer' = 'silver',
# MAGIC   'domain' = 'finance',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.reviews SET TAGS (
# MAGIC   'medallion_layer' = 'silver',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.products SET TAGS (
# MAGIC   'medallion_layer' = 'silver',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'static',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.sellers SET TAGS (
# MAGIC   'medallion_layer' = 'silver',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'static',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.geolocation SET TAGS (
# MAGIC   'medallion_layer' = 'silver',
# MAGIC   'domain' = 'infrastructure',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'static',
# MAGIC   'data_classification' = 'public'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold: domain reflects which team would own consumption, not which
# MAGIC -- entity the table is built from. seller_scorecard is operations
# MAGIC -- (not marketing) because operational teams act on it; payment_analysis
# MAGIC -- is finance (not operations) because it answers a finance question.
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.monthly_revenue SET TAGS (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'domain' = 'finance',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.delivery_performance SET TAGS (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.customer_rfm SET TAGS (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'domain' = 'marketing',
# MAGIC   'pii' = 'true',
# MAGIC   'refresh_frequency' = 'weekly',
# MAGIC   'data_classification' = 'restricted'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.category_analytics SET TAGS (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'domain' = 'marketing',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.seller_scorecard SET TAGS (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'weekly',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.payment_analysis SET TAGS (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'domain' = 'finance',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.geographic_metrics SET TAGS (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.review_trends SET TAGS (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'domain' = 'operations',
# MAGIC   'pii' = 'false',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_classification' = 'internal'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pivot the tag rows into one row per table for at-a-glance review.
# MAGIC -- information_schema.table_tags is the per-catalog UC view.
# MAGIC -- Reference: https://docs.databricks.com/aws/en/sql/language-manual/information-schema/table_tags
# MAGIC
# MAGIC WITH tags_pivoted AS (
# MAGIC   SELECT
# MAGIC     schema_name AS table_schema,
# MAGIC     table_name,
# MAGIC     MAX(CASE WHEN tag_name = 'medallion_layer' THEN tag_value END) AS medallion_layer,
# MAGIC     MAX(CASE WHEN tag_name = 'domain' THEN tag_value END) AS domain,
# MAGIC     MAX(CASE WHEN tag_name = 'pii' THEN tag_value END) AS pii,
# MAGIC     MAX(CASE WHEN tag_name = 'refresh_frequency' THEN tag_value END) AS refresh_frequency,
# MAGIC     MAX(CASE WHEN tag_name = 'data_classification' THEN tag_value END) AS data_classification,
# MAGIC     COUNT(*) AS tag_count
# MAGIC   FROM olist_lakehouse_us.information_schema.table_tags
# MAGIC   WHERE schema_name IN ('bronze', 'silver', 'gold')
# MAGIC   GROUP BY schema_name, table_name
# MAGIC )
# MAGIC SELECT
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   medallion_layer,
# MAGIC   domain,
# MAGIC   pii,
# MAGIC   refresh_frequency,
# MAGIC   data_classification,
# MAGIC   CASE WHEN tag_count = 5 THEN '✅ ALL 5' ELSE CONCAT('⚠️ ', tag_count, '/5') END AS status
# MAGIC FROM tags_pivoted
# MAGIC ORDER BY
# MAGIC   CASE table_schema WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 WHEN 'gold' THEN 3 END,
# MAGIC   table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cross-check: any table in bronze/silver/gold WITHOUT all 5 tags?
# MAGIC WITH all_tables AS (
# MAGIC   SELECT table_schema, table_name
# MAGIC   FROM olist_lakehouse_us.information_schema.tables
# MAGIC   WHERE table_schema IN ('bronze', 'silver', 'gold')
# MAGIC     AND table_type = 'MANAGED'
# MAGIC ),
# MAGIC tag_counts AS (
# MAGIC   SELECT schema_name AS table_schema, table_name, COUNT(*) AS tag_count
# MAGIC   FROM olist_lakehouse_us.information_schema.table_tags
# MAGIC   WHERE schema_name IN ('bronze', 'silver', 'gold')
# MAGIC   GROUP BY schema_name, table_name
# MAGIC )
# MAGIC SELECT
# MAGIC   a.table_schema,
# MAGIC   a.table_name,
# MAGIC   COALESCE(t.tag_count, 0) AS tag_count
# MAGIC FROM all_tables a
# MAGIC LEFT JOIN tag_counts t USING (table_schema, table_name)
# MAGIC WHERE COALESCE(t.tag_count, 0) < 5
# MAGIC ORDER BY a.table_schema, a.table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demo query: the kind of question tags exist to answer.
# MAGIC -- "Show me every PII-bearing Gold table and how often it refreshes."
# MAGIC
# MAGIC SELECT
# MAGIC   schema_name AS table_schema,
# MAGIC   table_name,
# MAGIC   MAX(CASE WHEN tag_name = 'refresh_frequency' THEN tag_value END) AS refresh_frequency
# MAGIC FROM olist_lakehouse_us.information_schema.table_tags
# MAGIC WHERE schema_name = 'gold'
# MAGIC   AND table_name IN (
# MAGIC     SELECT table_name
# MAGIC     FROM olist_lakehouse_us.information_schema.table_tags
# MAGIC     WHERE schema_name = 'gold' AND tag_name = 'pii' AND tag_value = 'true'
# MAGIC   )
# MAGIC GROUP BY schema_name, table_name;

# COMMAND ----------

