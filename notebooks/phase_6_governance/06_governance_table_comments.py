# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Phase 6.1 — Table-level Comments
# MAGIC
# MAGIC Adds a one-line description to every Bronze, Silver, and Gold table in
# MAGIC `olist_lakehouse_us`. Comments render in Catalog Explorer, `DESCRIBE TABLE
# MAGIC EXTENDED`, and as the data-source description in AI/BI dashboards.
# MAGIC
# MAGIC Idempotent: safe to re-run after any `CREATE OR REPLACE TABLE`.
# MAGIC
# MAGIC Reference: Databricks SQL — COMMENT ON
# MAGIC https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-comment-on

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze: source-exact mirrors of CSVs. Comments encode the
# MAGIC -- "preserve source, don't clean" principle so it's discoverable
# MAGIC -- without reading the README.
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.bronze.orders IS
# MAGIC   'Source-exact mirror of olist_orders_dataset.csv. 99,441 rows. Auto Loader-ingested. Bronze preserves source: no dedup, no null handling, naive Brazil-local timestamps. See silver.orders for cleaned form.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.bronze.order_items IS
# MAGIC   'Source-exact mirror of olist_order_items_dataset.csv. 112,650 rows. Line-item grain (one row per item per order). Auto Loader-ingested.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.bronze.customers IS
# MAGIC   'Source-exact mirror of olist_customers_dataset.csv. 99,441 rows. Per-order grain — customer_id is per-order; customer_unique_id is the actual person.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.bronze.payments IS
# MAGIC   'Source-exact mirror of olist_order_payments_dataset.csv. Multiple payments per order possible. Demonstrates Auto Loader incremental ingestion (split into payments_batch1.csv + payments_batch2.csv).';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.bronze.reviews IS
# MAGIC   'Source-exact mirror of olist_order_reviews_dataset.csv. 99,224 rows. Contains 814 duplicate review_ids with different order_ids — preserved here, deduplicated to composite (review_id, order_id) PK in Silver.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.bronze.products IS
# MAGIC   'Source-exact mirror of olist_products_dataset.csv. 32,951 rows. Preserves the typo''d source columns: product_name_lenght, product_description_lenght, product_photos_qty (sic).';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.bronze.sellers IS
# MAGIC   'Source-exact mirror of olist_sellers_dataset.csv. 3,095 rows. 60% of sellers in São Paulo state — supply-side concentration.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.bronze.geolocation IS
# MAGIC   'Source-exact mirror of olist_geolocation_dataset.csv. 1,000,163 rows. Multiple lat/lng points per zip prefix and 261,831 full-row duplicates — aggregated to 19,010 zip-prefix centroids in silver.geolocation.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.bronze.category_translation IS
# MAGIC   'Source-exact mirror of product_category_name_translation.csv. 71 rows. 2 categories missing translations (pc_gamer, portateis_cozinha_e_preparadores_de_alimentos) — hand-mapped in Silver.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver: cleaned, typed, enriched. Comments document the
# MAGIC -- transformation contract (what got fixed) and any surfaced
# MAGIC -- anomalies (flags that exist because Bronze data was messy).
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.silver.orders IS
# MAGIC   'Cleaned orders with typed timestamps (TIMESTAMP_NTZ, Brazil-local), derived order_year/month/quarter, and order_status preserved. 99,441 rows. Use order_status = ''delivered'' for revenue/SLA metrics.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.silver.order_items IS
# MAGIC   'Cleaned line items with derived total_item_value = price + freight_value. 112,650 rows. Canonical revenue source for Gold layer.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.silver.customers IS
# MAGIC   'Cleaned customers at per-order grain. 99,441 rows. customer_unique_id is the deduplicated person identifier; customer_id is per-order. 96.88% of customer_unique_ids appear exactly once.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.silver.payments IS
# MAGIC   'Cleaned payments with derived payment_type_known flag, installments_normalized, and installment_bucket (1, 2-3, 4-6, 7-12, 13+). The not_defined payment_type from Bronze is preserved with payment_type_known = false.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.silver.reviews IS
# MAGIC   'Cleaned reviews with composite (review_id, order_id) PK, derived comment_length/title_length, and sentiment classification via silver.classify_review_sentiment UDF.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.silver.products IS
# MAGIC   'Cleaned products with corrected column names (lenght → length), English category_name_en joined from category_translation, and 2 hand-mapped translations for source-missing categories.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.silver.sellers IS
# MAGIC   'Cleaned sellers with normalized zip code prefix (leading zeros preserved as STRING) and city.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.silver.geolocation IS
# MAGIC   'Aggregated to one centroid per zip_code_prefix. lat_centroid = AVG(lat), lng_centroid = AVG(lng), with source_point_count and MODE(city/state). 1M Bronze points → 19,010 Silver rows. 47 outside-Brazil points filtered before aggregation.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold: business-question-aligned aggregates. Comments name the
# MAGIC -- business question, the grain, and any caveats consumers must know
# MAGIC -- (RFM degeneracy, min-volume thresholds, sparse rows).
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.gold.monthly_revenue IS
# MAGIC   'Monthly revenue by category. Grain: (month_start, category_name_en). Source: silver.order_items (canonical revenue = price + freight_value, excludes installment fees). delivered orders only. ~1,800 rows. Canonical total: 15,419,773.75 BRL.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.gold.delivery_performance IS
# MAGIC   'Delivery SLA metrics by seller_state. Grain: seller_state with ≥10 orders (19 rows). Wide-format SLA bucket counts via silver.delivery_sla_status UDF. delivered orders only.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.gold.customer_rfm IS
# MAGIC   'RFM segmentation per customer_unique_id. 93,358 rows. CAVEAT: 97% of customers have F=1, so F-score is structurally degenerate — use is_repeat_customer for actionable retention queries, not the F dimension alone.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.gold.category_analytics IS
# MAGIC   'Category-level revenue + review + delivery joined metrics. Grain: category_name_en (excludes ''unknown''). 73 rows. Uses DISTINCT (order_id, category) pre-aggregation to avoid review-fanout.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.gold.seller_scorecard IS
# MAGIC   'Seller composite score 0-100 = 40% reviews + 30% delivery speed + 30% volume (capped). Grain: seller_id with ≥5 orders (1,766 rows). Components stored as separate columns for transparency. NTILE(5) tier classification.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.gold.payment_analysis IS
# MAGIC   'Payment type × installment bucket grain. 8 rows — sparse because only credit_card supports installments. Items-vs-payments reconciliation lives here only.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.gold.geographic_metrics IS
# MAGIC   'Customer-state to seller-state route metrics. Grain: (customer_state, seller_state) with ≥5 orders (274 rows). Hub-and-spoke finding: SP exports 1.73× imports; SP→SP is 26% of total marketplace revenue.';
# MAGIC
# MAGIC COMMENT ON TABLE olist_lakehouse_us.gold.review_trends IS
# MAGIC   'Review score trends by month. Grain: review_month_start (23 rows). At month grain, corr(delivery_days, score) = -0.91 — the speed-vs-punctuality finding lives here.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify every table has a non-NULL, non-empty comment.
# MAGIC -- information_schema.tables is the standard SQL-spec catalog
# MAGIC -- view; in UC it surfaces metastore-wide table metadata.
# MAGIC -- Reference: https://docs.databricks.com/aws/en/sql/language-manual/information-schema/tables
# MAGIC
# MAGIC SELECT
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   CASE
# MAGIC     WHEN comment IS NULL OR LENGTH(TRIM(comment)) = 0 THEN '❌ MISSING'
# MAGIC     WHEN LENGTH(comment) < 30 THEN '⚠️ TOO SHORT'
# MAGIC     ELSE '✅ OK'
# MAGIC   END AS comment_status,
# MAGIC   LENGTH(comment) AS comment_length,
# MAGIC   comment
# MAGIC FROM olist_lakehouse_us.information_schema.tables
# MAGIC WHERE table_schema IN ('bronze', 'silver', 'gold')
# MAGIC   AND table_type = 'MANAGED'
# MAGIC ORDER BY
# MAGIC   CASE table_schema WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 WHEN 'gold' THEN 3 END,
# MAGIC   table_name;

# COMMAND ----------

