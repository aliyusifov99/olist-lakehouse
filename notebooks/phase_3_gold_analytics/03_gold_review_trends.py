# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Review Trends
# MAGIC
# MAGIC **Layer:** Gold
# MAGIC **Source tables:** `silver.reviews`, `silver.orders`
# MAGIC **Target table:** `olist_lakehouse_us.gold.review_trends`
# MAGIC **Business question:** Q8 — What is the distribution of review scores over time? Is satisfaction improving or declining? (Per-category 1-star drivers are answered in `gold.category_analytics`.)
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC Aggregates reviews by **month** (using `review_creation_date`), producing one
# MAGIC row per month with score distribution, sentiment bucket counts, comment-engagement
# MAGIC metrics, and average delivery days for orders reviewed in that month. The
# MAGIC delivery-days column enables month-level correlation between review quality
# MAGIC and delivery performance.
# MAGIC
# MAGIC ## Grain & keys
# MAGIC
# MAGIC | Column | Role |
# MAGIC |---|---|
# MAGIC | `review_month_start` (DATE) | Primary key — first day of the month, derived from `review_creation_date` |
# MAGIC
# MAGIC ## Filter rules
# MAGIC
# MAGIC - **`order_status = 'delivered'` only** — non-delivered orders shouldn't shape
# MAGIC   satisfaction metrics. This filter is applied via the orders join.
# MAGIC - **All reviews retained** including the 814 duplicate review_ids from Phase 2.
# MAGIC   Each `(review_id, order_id)` row contributes to the month it was created in.
# MAGIC   See limitations.
# MAGIC
# MAGIC ## Why review_creation_date, not order_purchase_ts
# MAGIC
# MAGIC This table answers "what was satisfaction like in month X" — a question about
# MAGIC the *signal* generated in that month. A 1-star review submitted in October 2018
# MAGIC about an August 2018 order belongs in October's review trend, because that's
# MAGIC when the dissatisfaction was expressed. Reviewers' submission timing is the
# MAGIC natural unit of analysis for satisfaction trends.
# MAGIC
# MAGIC The trade-off: this table cannot be directly joined to `gold.monthly_revenue`
# MAGIC on `month_start` to compute "satisfaction per BRL of revenue" — the months
# MAGIC don't align. Cross-month correlation between review trends and revenue trends
# MAGIC needs explicit lag handling.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why this shape
# MAGIC
# MAGIC **Wide-format score buckets.** Five columns (`score_1_count` ... `score_5_count`)
# MAGIC plus their percentages. Same pattern as `gold.delivery_performance`'s SLA buckets.
# MAGIC Single row per month renders cleanly as a stacked-bar time series in Databricks
# MAGIC SQL widgets without a pivot step.
# MAGIC
# MAGIC **Sentiment buckets only on commented reviews.** Same approach as
# MAGIC `gold.category_analytics` — the `classify_review_sentiment` UDF distinguishes
# MAGIC `promoter` from `positive` based on comment presence. Aggregating sentiment
# MAGIC across no-comment reviews would collapse this distinction. Score-based metrics
# MAGIC use the full review population; sentiment-bucket metrics use the commented
# MAGIC subset.
# MAGIC
# MAGIC **Inner-join orders for delivery_days.** Reviews without a matching delivered
# MAGIC order are excluded. This is the right scope for satisfaction-with-delivery
# MAGIC analysis, but means `review_count` here will be lower than the population in
# MAGIC `silver.reviews`. The validation cells reconcile this.
# MAGIC
# MAGIC **Multi-row reviews counted per row, not per review_id.** The 814
# MAGIC `(review_id, order_id)` duplicates from Phase 2 each contribute their score
# MAGIC to the month's average. This is correct for "satisfaction signal per order"
# MAGIC purposes — if a review is attached to 3 orders, all 3 orders had that experience.
# MAGIC
# MAGIC [`classify_review_sentiment` UDF reference](https://docs.databricks.com/en/udf/unity-catalog.html)
# MAGIC [DATE_TRUNC reference](https://docs.databricks.com/en/sql/language-manual/functions/date_trunc.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.gold.review_trends
# MAGIC USING DELTA
# MAGIC COMMENT 'Review metrics aggregated by month (using review_creation_date). Delivered orders only. Score-based metrics use full review population; sentiment-bucket metrics use commented subset only. Joins silver.orders for delivery_days, enabling per-month review-vs-delivery correlation. PK: review_month_start.'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'gold',
# MAGIC   'medallion.layer' = 'gold',
# MAGIC   'source.timezone' = 'America/Sao_Paulo'
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC -- Combine reviews + delivered orders, with the time pivot
# MAGIC reviews_with_orders AS (
# MAGIC   SELECT
# MAGIC     DATE_TRUNC('MONTH', r.review_created_at)::DATE   AS review_month_start,
# MAGIC     r.review_id,
# MAGIC     r.order_id,
# MAGIC     r.review_score,
# MAGIC     r.sentiment,
# MAGIC     r.comment_length,
# MAGIC     r.title_length,
# MAGIC     o.delivery_days,
# MAGIC     o.delivery_delay_days,
# MAGIC     o.is_late_delivery
# MAGIC   FROM olist_lakehouse_us.silver.reviews         r
# MAGIC   INNER JOIN olist_lakehouse_us.silver.orders   o ON r.order_id = o.order_id
# MAGIC   WHERE o.order_status = 'delivered'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   review_month_start,
# MAGIC
# MAGIC   -- Volume
# MAGIC   COUNT(*)                                                                    AS review_count,
# MAGIC   COUNT(DISTINCT review_id)                                                   AS distinct_reviews,
# MAGIC   COUNT(DISTINCT order_id)                                                    AS distinct_orders_reviewed,
# MAGIC
# MAGIC   -- Score-based metrics (full review population)
# MAGIC   ROUND(AVG(review_score), 3)                                                 AS avg_review_score,
# MAGIC   ROUND(PERCENTILE(review_score, 0.5), 1)                                     AS median_review_score,
# MAGIC
# MAGIC   -- Score bucket counts (wide format)
# MAGIC   SUM(CASE WHEN review_score = 1 THEN 1 ELSE 0 END)                           AS score_1_count,
# MAGIC   SUM(CASE WHEN review_score = 2 THEN 1 ELSE 0 END)                           AS score_2_count,
# MAGIC   SUM(CASE WHEN review_score = 3 THEN 1 ELSE 0 END)                           AS score_3_count,
# MAGIC   SUM(CASE WHEN review_score = 4 THEN 1 ELSE 0 END)                           AS score_4_count,
# MAGIC   SUM(CASE WHEN review_score = 5 THEN 1 ELSE 0 END)                           AS score_5_count,
# MAGIC
# MAGIC   -- Score bucket percentages (sum to 100% per row, modulo rounding)
# MAGIC   ROUND(SUM(CASE WHEN review_score = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS score_1_pct,
# MAGIC   ROUND(SUM(CASE WHEN review_score = 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS score_2_pct,
# MAGIC   ROUND(SUM(CASE WHEN review_score = 3 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS score_3_pct,
# MAGIC   ROUND(SUM(CASE WHEN review_score = 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS score_4_pct,
# MAGIC   ROUND(SUM(CASE WHEN review_score = 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS score_5_pct,
# MAGIC
# MAGIC   -- Composite metrics
# MAGIC   ROUND(SUM(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS low_review_pct,
# MAGIC   ROUND(SUM(CASE WHEN review_score >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS high_review_pct,
# MAGIC
# MAGIC   -- Comment engagement (full population)
# MAGIC   ROUND(AVG(comment_length), 1)                                               AS avg_comment_length_chars,
# MAGIC   ROUND(SUM(CASE WHEN comment_length > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_with_comment,
# MAGIC   ROUND(SUM(CASE WHEN title_length > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)   AS pct_with_title,
# MAGIC
# MAGIC   -- Sentiment bucket counts (commented subset only)
# MAGIC   COUNT(CASE WHEN comment_length > 0 THEN 1 END)                              AS commented_review_count,
# MAGIC   SUM(CASE WHEN sentiment = 'promoter'        THEN 1 ELSE 0 END)              AS promoter_count,
# MAGIC   SUM(CASE WHEN sentiment = 'positive'        THEN 1 ELSE 0 END)              AS positive_count,
# MAGIC   SUM(CASE WHEN sentiment = 'neutral'         THEN 1 ELSE 0 END)              AS neutral_count,
# MAGIC   SUM(CASE WHEN sentiment = 'mixed_negative'  THEN 1 ELSE 0 END)              AS mixed_negative_count,
# MAGIC   SUM(CASE WHEN sentiment = 'negative'        THEN 1 ELSE 0 END)              AS negative_count,
# MAGIC
# MAGIC   -- Delivery context for the orders being reviewed
# MAGIC   ROUND(AVG(delivery_days), 1)                                                AS avg_delivery_days_for_reviewed_orders,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) * 100.0
# MAGIC     / NULLIF(SUM(CASE WHEN is_late_delivery IS NOT NULL THEN 1 ELSE 0 END), 0),
# MAGIC     2
# MAGIC   )                                                                           AS late_delivery_pct_for_reviewed_orders,
# MAGIC
# MAGIC   -- Lineage
# MAGIC   CURRENT_TIMESTAMP() AS _aggregated_at
# MAGIC
# MAGIC FROM reviews_with_orders
# MAGIC GROUP BY review_month_start
# MAGIC ORDER BY review_month_start;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC 1. **Row count** — should be in the 25-30 range (Olist data spans Sep 2016 to Oct 2018, ~25 months).
# MAGIC 2. **PK uniqueness** — `review_month_start` is the PK.
# MAGIC 3. **Score-bucket sum invariant** — `score_1_pct + score_2_pct + score_3_pct + score_4_pct + score_5_pct` should equal 100 (±0.05 for rounding) on every row.
# MAGIC 4. **Population avg score reconciliation** — volume-weighted average across all months should match Phase 2's audit figure (~4.09).
# MAGIC 5. **Reconciliation against `silver.reviews`** — `SUM(review_count)` should match the count of `silver.reviews` rows where the matching order is delivered. Phase 2 noted ~99,224 review rows and 768 orders without reviews — so this number should be close to ~95K (delivered + reviewed).
# MAGIC 6. **Trend direction sanity** — eyeball the time series for any obvious anomalies (e.g., one-off month with extreme drop or spike).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                              AS total_months,
# MAGIC   COUNT(DISTINCT review_month_start)                    AS distinct_months,
# MAGIC   MIN(review_month_start)                               AS first_month,
# MAGIC   MAX(review_month_start)                               AS last_month,
# MAGIC   COUNT(*) FILTER (
# MAGIC     WHERE ABS(
# MAGIC       (score_1_pct + score_2_pct + score_3_pct + score_4_pct + score_5_pct) - 100
# MAGIC     ) > 0.05
# MAGIC   )                                                     AS rows_violating_score_sum
# MAGIC FROM olist_lakehouse_us.gold.review_trends;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Volume-weighted national average should match Phase 2's 4.09
# MAGIC SELECT
# MAGIC   ROUND(SUM(avg_review_score * review_count) / SUM(review_count), 3)         AS volume_weighted_avg_score,
# MAGIC   SUM(review_count)                                                          AS total_review_rows,
# MAGIC   SUM(distinct_reviews)                                                      AS sum_distinct_reviews,
# MAGIC   SUM(distinct_orders_reviewed)                                              AS sum_distinct_orders,
# MAGIC   ROUND(SUM(low_review_pct * review_count) / SUM(review_count), 2)           AS national_low_review_pct,
# MAGIC   ROUND(SUM(high_review_pct * review_count) / SUM(review_count), 2)          AS national_high_review_pct,
# MAGIC   ROUND(SUM(pct_with_comment * review_count) / SUM(review_count), 2)         AS national_pct_with_comment
# MAGIC FROM olist_lakehouse_us.gold.review_trends;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Is satisfaction improving or declining? The dashboard's main question.
# MAGIC SELECT
# MAGIC   review_month_start,
# MAGIC   review_count,
# MAGIC   avg_review_score,
# MAGIC   score_5_pct                              AS pct_5_star,
# MAGIC   low_review_pct                           AS pct_1_or_2_star,
# MAGIC   pct_with_comment,
# MAGIC   avg_delivery_days_for_reviewed_orders,
# MAGIC   late_delivery_pct_for_reviewed_orders
# MAGIC FROM olist_lakehouse_us.gold.review_trends
# MAGIC ORDER BY review_month_start;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The cross-dimensional question this Gold table earns its place by answering:
# MAGIC -- across months, does delivery quality predict review quality?
# MAGIC SELECT
# MAGIC   ROUND(CORR(avg_delivery_days_for_reviewed_orders, avg_review_score), 3)    AS corr_delivery_days_vs_avg_score,
# MAGIC   ROUND(CORR(avg_delivery_days_for_reviewed_orders, low_review_pct), 3)      AS corr_delivery_days_vs_low_review_pct,
# MAGIC   ROUND(CORR(late_delivery_pct_for_reviewed_orders, avg_review_score), 3)    AS corr_late_pct_vs_avg_score,
# MAGIC   COUNT(*)                                                                   AS months_in_correlation
# MAGIC FROM olist_lakehouse_us.gold.review_trends
# MAGIC WHERE review_count >= 100;  -- exclude tiny early/late months with few reviews

# COMMAND ----------

