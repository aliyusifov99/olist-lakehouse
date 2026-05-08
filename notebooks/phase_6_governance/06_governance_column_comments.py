# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Phase 6 — Column-level Comments
# MAGIC
# MAGIC Adds comments to columns where a consumer could plausibly use them
# MAGIC incorrectly without one. Selection rule: a column gets a comment if its
# MAGIC name alone doesn't fully convey its semantics, units, or population.
# MAGIC
# MAGIC Reference: Databricks SQL — ALTER TABLE ... ALTER COLUMN
# MAGIC https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-table-manage-column
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver.customers: the two-IDs gotcha
# MAGIC ALTER TABLE olist_lakehouse_us.silver.customers ALTER COLUMN customer_id
# MAGIC   COMMENT 'Per-order customer identifier. NOT a person — a new value is generated for every order placed by the same person. Use customer_unique_id for person-level analysis.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.customers ALTER COLUMN customer_unique_id
# MAGIC   COMMENT 'Stable per-person identifier. Use this for repeat-customer, RFM, and lifetime-value analysis. 96.88% of values appear exactly once in the dataset.';
# MAGIC
# MAGIC -- silver.orders: status semantics matter for every downstream join
# MAGIC ALTER TABLE olist_lakehouse_us.silver.orders ALTER COLUMN order_status
# MAGIC   COMMENT 'Order lifecycle state. Canonical filter for revenue/SLA metrics is order_status = ''delivered'' — non-delivered orders have NULL delivery timestamps and would corrupt aggregates.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.orders ALTER COLUMN order_purchase_ts
# MAGIC   COMMENT 'Naive Brazil-local timestamp (America/Sao_Paulo), TIMESTAMP_NTZ. No timezone conversion applied. Source.timezone TBLPROPERTY documents this contract metastore-wide.';
# MAGIC
# MAGIC -- silver.order_items: revenue source of truth
# MAGIC ALTER TABLE olist_lakehouse_us.silver.order_items ALTER COLUMN total_item_value
# MAGIC   COMMENT 'Canonical line-item revenue: price + freight_value, in BRL. Excludes payment-side installment financing fees. Sum across delivered orders = 15,419,773.75 BRL (project canonical figure).';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.order_items ALTER COLUMN price
# MAGIC   COMMENT 'Product price in BRL, line-item grain. Excludes freight.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.order_items ALTER COLUMN freight_value
# MAGIC   COMMENT 'Shipping cost in BRL, line-item grain. Allocated per item, not per order. Average ~14% of price.';
# MAGIC
# MAGIC -- silver.payments: surfaced anomaly flags
# MAGIC ALTER TABLE olist_lakehouse_us.silver.payments ALTER COLUMN payment_type_known
# MAGIC   COMMENT 'False when source payment_type was ''not_defined''. Preserved as an explicit flag rather than dropped — surfaces the source-quality issue downstream.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.payments ALTER COLUMN installment_bucket
# MAGIC   COMMENT 'Bucketed installments: 1, 2-3, 4-6, 7-12, 13+. Average payment value scales monotonically with bucket: 112 BRL (1) → 414 BRL (13+).';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.payments ALTER COLUMN payment_value
# MAGIC   COMMENT 'Payment amount in BRL, per payment instrument (orders may have multiple payments). Includes installment financing fees that are NOT in order_items.total_item_value.';
# MAGIC
# MAGIC -- silver.reviews: composite key + UDF-derived field
# MAGIC ALTER TABLE olist_lakehouse_us.silver.reviews ALTER COLUMN review_id
# MAGIC   COMMENT 'NOT unique on its own. 814 review_ids appear with different order_ids in source — composite (review_id, order_id) is the PK.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.reviews ALTER COLUMN sentiment
# MAGIC   COMMENT 'Derived via silver.classify_review_sentiment UDF. Buckets: promoter, positive, neutral, mixed_negative (score 3 with comment >50 chars), negative.';
# MAGIC
# MAGIC -- silver.products: category-translation gotcha
# MAGIC ALTER TABLE olist_lakehouse_us.silver.products ALTER COLUMN category_name_en
# MAGIC   COMMENT 'English category name from category_translation join. ''unknown'' indicates source had NULL product_category_name (610 products). Filter out for category analytics.';
# MAGIC
# MAGIC -- silver.geolocation: aggregation contract
# MAGIC ALTER TABLE olist_lakehouse_us.silver.geolocation ALTER COLUMN lat_centroid
# MAGIC   COMMENT 'AVG(lat) across source points within zip_code_prefix, after filtering 47 points outside Brazil bounding box. NOT a single observed point.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.silver.geolocation ALTER COLUMN source_point_count
# MAGIC   COMMENT 'Number of source rows aggregated into this centroid. Use as a confidence weight — low counts indicate fewer observations.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold.monthly_revenue: which revenue definition?
# MAGIC ALTER TABLE olist_lakehouse_us.gold.monthly_revenue ALTER COLUMN total_revenue
# MAGIC   COMMENT 'Canonical revenue: SUM(price + freight_value) from silver.order_items, delivered orders only. Excludes installment fees. Bit-for-bit reconciles with Phase 4 SDP rebuild.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.monthly_revenue ALTER COLUMN order_count
# MAGIC   COMMENT 'Distinct delivered orders contributing to this (month, category) row. Note: same order can contribute to multiple rows if it has line items in multiple categories — summing this column overcounts orders by ~0.5%.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.monthly_revenue ALTER COLUMN month_start
# MAGIC   COMMENT 'First day of month, derived from order_purchase_ts (Brazil-local). Use this column for time-series grouping, not order_year/order_month separately.';
# MAGIC
# MAGIC -- gold.customer_rfm: the F-degeneracy caveat at the column level
# MAGIC ALTER TABLE olist_lakehouse_us.gold.customer_rfm ALTER COLUMN customer_unique_id
# MAGIC   COMMENT 'Stable per-person identifier from silver.customers. Anonymized in source. PK of this table.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.customer_rfm ALTER COLUMN monetary
# MAGIC   COMMENT 'Total lifetime spend in BRL: SUM(price + freight_value) from delivered orders. Excludes installment fees.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.customer_rfm ALTER COLUMN frequency
# MAGIC   COMMENT 'Distinct delivered order count. STRUCTURALLY DEGENERATE: 97% of customers have frequency = 1. Use is_repeat_customer for actionable retention queries.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.customer_rfm ALTER COLUMN is_repeat_customer
# MAGIC   COMMENT 'True iff frequency >= 2. The actionable retention flag — frequency itself is structurally degenerate (see frequency comment).';
# MAGIC
# MAGIC -- gold.delivery_performance: avg_delay_days sign convention
# MAGIC ALTER TABLE olist_lakehouse_us.gold.delivery_performance ALTER COLUMN avg_delay_days
# MAGIC   COMMENT 'AVG(actual_delivery - estimated_delivery) in days. NEGATIVE = delivered earlier than promised (the universal pattern in this dataset). All 19 states have negative avg_delay_days.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.delivery_performance ALTER COLUMN strict_late_rate_pct
# MAGIC   COMMENT 'Pct of delivered orders where actual_delivery > estimated_delivery by ANY margin. Stricter than sla_late_rate_pct (which has ±2-day grace).';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.delivery_performance ALTER COLUMN sla_late_rate_pct
# MAGIC   COMMENT 'Pct of delivered orders classified ''slightly_late'' or ''very_late'' by silver.delivery_sla_status UDF (±2-day grace period). Closer to project plan''s ~6% target.';
# MAGIC
# MAGIC -- gold.seller_scorecard: composite score formula
# MAGIC ALTER TABLE olist_lakehouse_us.gold.seller_scorecard ALTER COLUMN composite_score
# MAGIC   COMMENT 'Bounded 0-100. Formula: 40 × (avg_review_score / 5) + delivery_speed_points (0-30) + 30 × LEAST(order_count / 100, 1). Components stored separately for transparency.';
# MAGIC
# MAGIC
# MAGIC -- gold.geographic_metrics: route grain semantics
# MAGIC ALTER TABLE olist_lakehouse_us.gold.geographic_metrics ALTER COLUMN customer_state
# MAGIC   COMMENT 'Customer 2-letter Brazilian state code. Combined with seller_state forms the route grain (≥5 orders threshold).';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.geographic_metrics ALTER COLUMN seller_state
# MAGIC   COMMENT 'Seller 2-letter Brazilian state code. SP origin accounts for 60% of all marketplace revenue across destinations.';
# MAGIC
# MAGIC ALTER TABLE olist_lakehouse_us.gold.geographic_metrics ALTER COLUMN total_revenue
# MAGIC   COMMENT 'Per-route revenue. Filtered by min-volume threshold (≥5 orders), so sum across all rows is ~74K BRL less than gold.monthly_revenue total. This is expected — sub-threshold routes are dropped, not aggregated elsewhere.';
# MAGIC
# MAGIC -- gold.review_trends: which population for the late-pct?
# MAGIC ALTER TABLE olist_lakehouse_us.gold.review_trends ALTER COLUMN late_delivery_pct_for_reviewed_orders
# MAGIC   COMMENT 'Late-delivery pct restricted to orders that received a review. Uses the SAME population as the review-score columns — required for the corr(delivery_days, score) = -0.91 finding to be numerically interpretable, not just visually suggestive.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Audit which columns got comments and where coverage is intentionally
# MAGIC -- thin. information_schema.columns is the per-catalog SQL-spec view.
# MAGIC -- Reference: https://docs.databricks.com/aws/en/sql/language-manual/information-schema/columns
# MAGIC
# MAGIC WITH commented AS (
# MAGIC   SELECT
# MAGIC     table_schema,
# MAGIC     table_name,
# MAGIC     COUNT(*) AS total_columns,
# MAGIC     SUM(CASE WHEN comment IS NOT NULL AND LENGTH(TRIM(comment)) > 0 THEN 1 ELSE 0 END) AS commented_columns
# MAGIC   FROM olist_lakehouse_us.information_schema.columns
# MAGIC   WHERE table_schema IN ('bronze', 'silver', 'gold')
# MAGIC     AND table_name IN (
# MAGIC       SELECT table_name FROM olist_lakehouse_us.information_schema.tables
# MAGIC       WHERE table_schema IN ('bronze', 'silver', 'gold') AND table_type = 'MANAGED'
# MAGIC     )
# MAGIC   GROUP BY table_schema, table_name
# MAGIC )
# MAGIC SELECT
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   total_columns,
# MAGIC   commented_columns,
# MAGIC   ROUND(100.0 * commented_columns / total_columns, 1) AS pct_commented
# MAGIC FROM commented
# MAGIC ORDER BY
# MAGIC   CASE table_schema WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 WHEN 'gold' THEN 3 END,
# MAGIC   table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Spot-check: list every commented column with its comment, in the order
# MAGIC -- a reviewer would scan. This is the "show me what you actually documented" view.
# MAGIC
# MAGIC SELECT
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   column_name,
# MAGIC   comment
# MAGIC FROM olist_lakehouse_us.information_schema.columns
# MAGIC WHERE table_schema IN ('silver', 'gold')
# MAGIC   AND comment IS NOT NULL
# MAGIC   AND LENGTH(TRIM(comment)) > 0
# MAGIC ORDER BY
# MAGIC   CASE table_schema WHEN 'silver' THEN 1 WHEN 'gold' THEN 2 END,
# MAGIC   table_name,
# MAGIC   column_name;

# COMMAND ----------

