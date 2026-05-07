# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Category Analytics
# MAGIC
# MAGIC **Layer:** Gold
# MAGIC **Source tables:** `silver.orders`, `silver.order_items`, `silver.products`, `silver.reviews`
# MAGIC **UDF used:** `silver.classify_review_sentiment`
# MAGIC **Target table:** `olist_lakehouse_us.gold.category_analytics`
# MAGIC **Business question:** Q4 — Top 10 categories by revenue and order count? Which categories have the worst review scores, and is there a correlation with late delivery?
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC Aggregates delivered orders to the **product-category** grain, joining revenue
# MAGIC metrics, delivery performance, and review sentiment into a single denormalized
# MAGIC row per category. This is the first Gold table that combines all three
# MAGIC dimensions (revenue / delivery / reviews) — supporting cross-dimensional
# MAGIC correlation analysis at the dashboard layer.
# MAGIC
# MAGIC ## Grain & keys
# MAGIC
# MAGIC | Column | Role |
# MAGIC |---|---|
# MAGIC | `category_name_en` (STRING) | Primary key — English category name from `silver.products` |
# MAGIC
# MAGIC ## Filter rules
# MAGIC
# MAGIC - `order_status = 'delivered'` only
# MAGIC - `category_name_en != 'unknown'` — the 610 products with missing source category
# MAGIC   data (1.28% of revenue) are excluded. Category analytics by definition requires
# MAGIC   a known category. The 'unknown' rows remain in `gold.monthly_revenue` for
# MAGIC   total reconciliation purposes.
# MAGIC - Categories with fewer than 50 orders are kept (no min-volume threshold) —
# MAGIC   small categories matter for category-completeness analysis. Per-category
# MAGIC   sample-size context is exposed via the `order_count` column.
# MAGIC
# MAGIC ## Review metric scoping
# MAGIC
# MAGIC - **Score-based metrics** (`avg_review_score`, `low_review_rate_pct`,
# MAGIC   `high_review_rate_pct`) are computed on the full review population for the
# MAGIC   category — every review counts.
# MAGIC - **Sentiment-bucket metrics** (`promoter_count`, `negative_count`, etc.) are
# MAGIC   computed only on the **commented subset** (reviews with `comment_length > 0`),
# MAGIC   because the `classify_review_sentiment` UDF distinguishes `promoter` from
# MAGIC   `positive` based on comment presence. Aggregating sentiment buckets across
# MAGIC   no-comment reviews would collapse the bucket distinction.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why this shape
# MAGIC
# MAGIC **Order × category grain in the inner CTE, category grain in the output.**
# MAGIC A single order can span multiple categories (a customer buying health_beauty
# MAGIC + furniture_decor in one order). We expand to (order_id, category) pairs first,
# MAGIC then aggregate to category. Each review attributes to *every* category in its
# MAGIC order — the review reflects order-level experience, not item-level.
# MAGIC
# MAGIC **`order_items` fanout handled explicitly.** Each line item adds a row in
# MAGIC `order_items`. Joining to `reviews` naively would multiply review counts by
# MAGIC line-item count. The CTE structure aggregates to (order, category) before
# MAGIC touching reviews, so each (order, category) pair is one row.
# MAGIC
# MAGIC **Two review-score lenses, not one.** `avg_review_score_all` covers every
# MAGIC review for the category; `avg_review_score_commented` filters to comment > 0.
# MAGIC Phase 2 found 59% of reviews have no comment text — half the population. The
# MAGIC score-only customers leave 5-stars (or 1-stars) without explanation; the
# MAGIC commented customers cared enough to type. Both subsets matter, but they
# MAGIC behave differently — commented customers tend to have more polarized scores.
# MAGIC
# MAGIC **No min-volume threshold.** Unlike `delivery_performance` (min 10 orders per
# MAGIC state), this table includes all categories regardless of order count. The
# MAGIC order_count column itself is the volume signal — let the dashboard decide
# MAGIC whether to filter `WHERE order_count >= N`. Keeping rare categories visible
# MAGIC matters for category-coverage analysis.
# MAGIC
# MAGIC **Late delivery from `is_late_delivery` (strict definition), not the SLA UDF.**
# MAGIC The strict boolean is more sensitive to the late/on-time signal we want to
# MAGIC correlate against review scores — even a 1-day late delivery may affect a review.
# MAGIC The `slightly_late`/`very_late` SLA buckets would underweight marginal lateness.
# MAGIC
# MAGIC [CORR function reference](https://docs.databricks.com/en/sql/language-manual/functions/corr.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.gold.category_analytics
# MAGIC USING DELTA
# MAGIC COMMENT 'Category-level revenue, delivery, and review metrics. Delivered orders only. Excludes "unknown" category (1.28% of revenue retained in gold.monthly_revenue). Each review attributes to every category in its order (multi-category orders count their review toward all categories). Sentiment-bucket counts use the commented-review subset; score-based metrics use full review population. PK: category_name_en.'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'gold',
# MAGIC   'medallion.layer' = 'gold',
# MAGIC   'source.timezone' = 'America/Sao_Paulo'
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC -- Expand to (order_id, category) pairs to handle multi-category orders cleanly
# MAGIC order_category_pairs AS (
# MAGIC   SELECT DISTINCT
# MAGIC     o.order_id,
# MAGIC     o.customer_id,
# MAGIC     o.is_late_delivery,
# MAGIC     o.delivery_days,
# MAGIC     o.delivery_delay_days,
# MAGIC     p.category_name_en
# MAGIC   FROM olist_lakehouse_us.silver.orders            o
# MAGIC   INNER JOIN olist_lakehouse_us.silver.order_items oi ON o.order_id = oi.order_id
# MAGIC   INNER JOIN olist_lakehouse_us.silver.products    p  ON oi.product_id = p.product_id
# MAGIC   WHERE o.order_status = 'delivered'
# MAGIC     AND p.category_name_en != 'unknown'
# MAGIC ),
# MAGIC
# MAGIC -- Per-category revenue/delivery aggregates (one row per category)
# MAGIC revenue_delivery AS (
# MAGIC   SELECT
# MAGIC     p.category_name_en,
# MAGIC     COUNT(DISTINCT o.order_id)              AS order_count,
# MAGIC     COUNT(DISTINCT o.customer_id)           AS customer_count,
# MAGIC     COUNT(*)                                AS line_item_count,
# MAGIC
# MAGIC     ROUND(SUM(oi.price), 2)                 AS product_revenue,
# MAGIC     ROUND(SUM(oi.freight_value), 2)         AS freight_revenue,
# MAGIC     ROUND(SUM(oi.total_item_value), 2)      AS total_revenue,
# MAGIC     ROUND(AVG(oi.price), 2)                 AS avg_item_price,
# MAGIC
# MAGIC     ROUND(AVG(o.delivery_days), 1)          AS avg_delivery_days,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN o.is_late_delivery THEN 1 ELSE 0 END) * 100.0
# MAGIC       / SUM(CASE WHEN o.is_late_delivery IS NOT NULL THEN 1 ELSE 0 END),
# MAGIC       2
# MAGIC     )                                       AS late_rate_pct
# MAGIC   FROM olist_lakehouse_us.silver.orders            o
# MAGIC   INNER JOIN olist_lakehouse_us.silver.order_items oi ON o.order_id = oi.order_id
# MAGIC   INNER JOIN olist_lakehouse_us.silver.products    p  ON oi.product_id = p.product_id
# MAGIC   WHERE o.order_status = 'delivered'
# MAGIC     AND p.category_name_en != 'unknown'
# MAGIC   GROUP BY p.category_name_en
# MAGIC ),
# MAGIC
# MAGIC -- Per-category review aggregates (joins via order_category_pairs to avoid line-item fanout)
# MAGIC reviews_aggregated AS (
# MAGIC   SELECT
# MAGIC     ocp.category_name_en,
# MAGIC
# MAGIC     -- Score-based metrics (full review population for this category)
# MAGIC     COUNT(r.review_id)                                                          AS review_count,
# MAGIC     ROUND(AVG(r.review_score), 2)                                               AS avg_review_score_all,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(r.review_id),
# MAGIC       2
# MAGIC     )                                                                           AS low_review_rate_pct,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN r.review_score >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(r.review_id),
# MAGIC       2
# MAGIC     )                                                                           AS high_review_rate_pct,
# MAGIC
# MAGIC     -- Score-based metrics (commented subset only)
# MAGIC     COUNT(CASE WHEN r.comment_length > 0 THEN r.review_id END)                  AS commented_review_count,
# MAGIC     ROUND(
# MAGIC       AVG(CASE WHEN r.comment_length > 0 THEN r.review_score END), 2
# MAGIC     )                                                                           AS avg_review_score_commented,
# MAGIC
# MAGIC     -- Sentiment bucket counts (commented subset only — UDF distinguishes 'promoter' from 'positive' by comment presence)
# MAGIC     SUM(CASE WHEN r.sentiment = 'promoter'        THEN 1 ELSE 0 END)            AS promoter_count,
# MAGIC     SUM(CASE WHEN r.sentiment = 'positive'        THEN 1 ELSE 0 END)            AS positive_count,
# MAGIC     SUM(CASE WHEN r.sentiment = 'neutral'         THEN 1 ELSE 0 END)            AS neutral_count,
# MAGIC     SUM(CASE WHEN r.sentiment = 'mixed_negative' THEN 1 ELSE 0 END)             AS mixed_negative_count,
# MAGIC     SUM(CASE WHEN r.sentiment = 'negative'        THEN 1 ELSE 0 END)            AS negative_count
# MAGIC   FROM order_category_pairs ocp
# MAGIC   INNER JOIN olist_lakehouse_us.silver.reviews r ON ocp.order_id = r.order_id
# MAGIC   GROUP BY ocp.category_name_en
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   rd.category_name_en,
# MAGIC
# MAGIC   -- Volume
# MAGIC   rd.order_count,
# MAGIC   rd.customer_count,
# MAGIC   rd.line_item_count,
# MAGIC
# MAGIC   -- Revenue (BRL)
# MAGIC   rd.product_revenue,
# MAGIC   rd.freight_revenue,
# MAGIC   rd.total_revenue,
# MAGIC   rd.avg_item_price,
# MAGIC   ROUND(rd.total_revenue / rd.order_count, 2) AS avg_order_value,
# MAGIC
# MAGIC   -- Delivery
# MAGIC   rd.avg_delivery_days,
# MAGIC   rd.late_rate_pct,
# MAGIC
# MAGIC   -- Reviews (score-based, full population)
# MAGIC   ra.review_count,
# MAGIC   rd.order_count - ra.review_count            AS orders_without_review,
# MAGIC   ra.avg_review_score_all,
# MAGIC   ra.low_review_rate_pct,
# MAGIC   ra.high_review_rate_pct,
# MAGIC
# MAGIC   -- Reviews (commented subset)
# MAGIC   ra.commented_review_count,
# MAGIC   ra.avg_review_score_commented,
# MAGIC
# MAGIC   -- Sentiment buckets (commented subset)
# MAGIC   ra.promoter_count,
# MAGIC   ra.positive_count,
# MAGIC   ra.neutral_count,
# MAGIC   ra.mixed_negative_count,
# MAGIC   ra.negative_count,
# MAGIC
# MAGIC   -- Lineage
# MAGIC   CURRENT_TIMESTAMP() AS _aggregated_at
# MAGIC
# MAGIC FROM revenue_delivery rd
# MAGIC LEFT JOIN reviews_aggregated ra ON rd.category_name_en = ra.category_name_en
# MAGIC ORDER BY rd.total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC 1. **Row count** — should be ~73 (74 distinct categories per Phase 2 minus the 'unknown' bucket). One row per category.
# MAGIC 2. **PK uniqueness** — `category_name_en` is the PK.
# MAGIC 3. **Revenue reconciliation** — `SUM(total_revenue)` should equal `gold.monthly_revenue` total minus the 'unknown' bucket revenue (~15.42M − ~198K = ~15.22M BRL).
# MAGIC 4. **Top-3 categories sanity** — `health_beauty`, `watches_gifts`, `bed_bath_table` per cell 8 of `monthly_revenue`.
# MAGIC 5. **Late-rate vs review-score correlation preview** — quick sanity that the categories with worst delivery do tend toward worse reviews.
# MAGIC 6. **Review coverage** — Phase 2 found 768 orders without reviews; the per-category `orders_without_review` should sum to a similar number (with multi-category fanout caveat).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                    AS total_rows,
# MAGIC   COUNT(DISTINCT category_name_en)            AS distinct_categories,
# MAGIC   ROUND(SUM(total_revenue), 2)                AS sum_total_revenue,
# MAGIC   ROUND(SUM(total_revenue) - 15221000, 0)     AS delta_vs_expected_15_22M,
# MAGIC   SUM(order_count)                            AS sum_orders,
# MAGIC   SUM(line_item_count)                        AS sum_line_items
# MAGIC FROM olist_lakehouse_us.gold.category_analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 by revenue
# MAGIC SELECT
# MAGIC   'top_10_by_revenue' AS view,
# MAGIC   category_name_en,
# MAGIC   order_count,
# MAGIC   total_revenue,
# MAGIC   avg_review_score_all,
# MAGIC   late_rate_pct
# MAGIC FROM olist_lakehouse_us.gold.category_analytics
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Worst 10 by review score (with min volume to avoid noise)
# MAGIC SELECT
# MAGIC   'worst_10_by_review' AS view,
# MAGIC   category_name_en,
# MAGIC   order_count,
# MAGIC   total_revenue,
# MAGIC   avg_review_score_all,
# MAGIC   low_review_rate_pct,
# MAGIC   late_rate_pct
# MAGIC FROM olist_lakehouse_us.gold.category_analytics
# MAGIC WHERE order_count >= 100
# MAGIC ORDER BY avg_review_score_all ASC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Population correlation across categories: does worse delivery predict worse reviews?
# MAGIC -- This is the answer the project plan's Q4 second-half asks for, computed at query time
# MAGIC -- rather than pre-stored in the table.
# MAGIC SELECT
# MAGIC   ROUND(CORR(late_rate_pct, avg_review_score_all),  3)    AS corr_late_vs_avg_score,
# MAGIC   ROUND(CORR(late_rate_pct, low_review_rate_pct),   3)    AS corr_late_vs_low_review_rate,
# MAGIC   ROUND(CORR(avg_delivery_days, avg_review_score_all), 3) AS corr_delivery_days_vs_avg_score,
# MAGIC   COUNT(*)                                                AS categories_in_correlation
# MAGIC FROM olist_lakehouse_us.gold.category_analytics
# MAGIC WHERE order_count >= 100;  -- exclude small categories with unstable rates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Should be in the ballpark of Phase 2's 768 orders-without-review finding
# MAGIC -- Note: with multi-category orders, an order missing a review counts in every
# MAGIC -- category it touched, so the sum here will be HIGHER than 768.
# MAGIC SELECT
# MAGIC   SUM(orders_without_review)              AS total_orders_without_review_with_fanout,
# MAGIC   ROUND(AVG(orders_without_review * 1.0 / order_count) * 100, 2) AS avg_pct_orders_without_review,
# MAGIC   MAX(orders_without_review)              AS worst_category_no_review
# MAGIC FROM olist_lakehouse_us.gold.category_analytics;

# COMMAND ----------

