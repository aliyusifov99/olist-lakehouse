# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Seller Scorecard
# MAGIC
# MAGIC **Layer:** Gold
# MAGIC **Source tables:** `silver.sellers`, `silver.order_items`, `silver.orders`, `silver.reviews`
# MAGIC **Target table:** `olist_lakehouse_us.gold.seller_scorecard`
# MAGIC **Business question:** Q5 — Which sellers have the highest revenue, best review scores, and fastest delivery? Which sellers are underperforming across all three dimensions?
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC Aggregates delivered orders to the **seller** grain, computing volume metrics
# MAGIC (order count, revenue), quality metrics (avg review score, late rate, avg
# MAGIC delivery days), and a composite 0-100 performance score using the project
# MAGIC plan's weighted formula. Sellers are tiered into quintiles (top_20pct →
# MAGIC bottom_20pct) for at-a-glance ranking.
# MAGIC
# MAGIC ## Grain & keys
# MAGIC
# MAGIC | Column | Role |
# MAGIC |---|---|
# MAGIC | `seller_id` (STRING) | Primary key — Phase 2's verified-unique seller identifier |
# MAGIC
# MAGIC ## Filter rules
# MAGIC
# MAGIC - `order_status = 'delivered'` only — non-delivered orders aren't seller-fault by default
# MAGIC - Min 5 delivered orders per seller for inclusion in the scorecard. Sellers below
# MAGIC   this threshold are excluded entirely (project plan threshold).
# MAGIC - All 3,095 sellers from `silver.sellers` are candidates; the threshold filters
# MAGIC   to active ones.
# MAGIC
# MAGIC ## Composite score formula (project plan)
# MAGIC
# MAGIC composite_score =
# MAGIC (COALESCE(avg_review_score, 3) / 5) × 40       -- 40% reviews
# MAGIC
# MAGIC delivery_speed_points                           -- 30% delivery (bucketed)
# MAGIC LEAST(order_count / 100, 1) × 30                -- 30% volume (capped at 100)
# MAGIC
# MAGIC where delivery_speed_points =
# MAGIC 30 if avg_delivery_days ≤ 7
# MAGIC 20 if avg_delivery_days ≤ 14
# MAGIC 10 if avg_delivery_days ≤ 21
# MAGIC 0 otherwise

# COMMAND ----------

# MAGIC %md
# MAGIC **Two design intent points worth knowing:**
# MAGIC
# MAGIC 1. **No-review sellers default to score 3.** `COALESCE(avg_review_score, 3)`
# MAGIC    treats a missing review average as average — sellers aren't penalized for
# MAGIC    having no reviews. Most sellers will have at least one review.
# MAGIC 2. **Volume cap at 100 orders.** The volume term saturates — a 100-order seller
# MAGIC    and a 10,000-order seller both get the full 30 points. The scorecard is a
# MAGIC    *quality-per-qualified-seller* ranking, not a pure size ranking.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why this shape
# MAGIC
# MAGIC **One row per seller, not per (seller, state).** A seller has exactly one home
# MAGIC state in `silver.sellers` — the seller-state metric we exposed in
# MAGIC `gold.delivery_performance` answered a *geographic* question; this table answers
# MAGIC a *who* question. Same source data, different lens.
# MAGIC
# MAGIC **Multi-seller order attribution: each line item belongs to one seller.** Unlike
# MAGIC the multi-category fanout in `category_analytics`, sellers don't fan out per-item
# MAGIC because each `order_items` row already names exactly one seller. So per-seller
# MAGIC aggregations on `order_items` are clean — no DISTINCT trickery needed.
# MAGIC
# MAGIC **Reviews attribute to all sellers in the order.** A multi-seller order's review
# MAGIC counts toward every seller in the order. This mirrors the category attribution
# MAGIC in `category_analytics` — the review reflects the whole order experience. Good sellers paired
# MAGIC with bad sellers in the same order will share blame; the noise washes out across
# MAGIC many orders.
# MAGIC
# MAGIC **LEFT JOIN to reviews, not INNER.** Phase 2 found 768 orders without reviews.
# MAGIC A seller whose orders are all unreviewed should still appear in the table with
# MAGIC NULL review metrics (not vanish). The `COALESCE(avg_review_score, 3)` in the
# MAGIC score formula handles the NULL case explicitly.
# MAGIC
# MAGIC **`is_qualified_for_ranking` not used here — the min-5 filter is a hard gate.**
# MAGIC We're applying the threshold inside the CTAS rather than via a flag column.
# MAGIC This matches the project plan and keeps the table narrowly scoped to "rankable
# MAGIC sellers." If we ever needed the full 3,095-seller view, that's a separate table
# MAGIC or a query against `silver.sellers` directly.
# MAGIC
# MAGIC [NTILE function reference](https://docs.databricks.com/en/sql/language-manual/functions/ntile.html)
# MAGIC [LEAST/GREATEST function reference](https://docs.databricks.com/en/sql/language-manual/functions/least.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.gold.seller_scorecard
# MAGIC USING DELTA
# MAGIC COMMENT 'Per-seller composite performance score and quintile tier. Min 5 delivered orders. Composite = 40% reviews + 30% delivery speed + 30% volume (capped at 100 orders). No-review sellers get score 3 in the formula. PK: seller_id.'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'gold',
# MAGIC   'medallion.layer' = 'gold',
# MAGIC   'source.timezone' = 'America/Sao_Paulo'
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC -- Per-seller aggregation across delivered orders
# MAGIC seller_metrics AS (
# MAGIC   SELECT
# MAGIC     s.seller_id,
# MAGIC     s.seller_state,
# MAGIC     s.seller_city,
# MAGIC
# MAGIC     -- Volume
# MAGIC     COUNT(DISTINCT oi.order_id)               AS order_count,
# MAGIC     COUNT(DISTINCT oi.product_id)             AS unique_products_sold,
# MAGIC     COUNT(*)                                  AS line_item_count,
# MAGIC
# MAGIC     -- Revenue
# MAGIC     ROUND(SUM(oi.price), 2)                   AS product_revenue,
# MAGIC     ROUND(SUM(oi.freight_value), 2)           AS freight_revenue,
# MAGIC     ROUND(SUM(oi.total_item_value), 2)        AS total_revenue,
# MAGIC     ROUND(AVG(oi.price), 2)                   AS avg_item_price,
# MAGIC
# MAGIC     -- Delivery (averaged across the seller's orders, deduped)
# MAGIC     -- DISTINCT to avoid per-line-item duplication of delivery metrics
# MAGIC     ROUND(AVG(DISTINCT_orders.delivery_days), 1)        AS avg_delivery_days,
# MAGIC     ROUND(AVG(DISTINCT_orders.delivery_delay_days), 1)  AS avg_delay_days,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN DISTINCT_orders.is_late_delivery THEN 1 ELSE 0 END) * 100.0
# MAGIC       / NULLIF(COUNT(DISTINCT DISTINCT_orders.order_id), 0),
# MAGIC       2
# MAGIC     )                                                   AS late_rate_pct,
# MAGIC
# MAGIC     -- Reviews (LEFT JOIN to reviews via the deduped order set)
# MAGIC     ROUND(AVG(r.review_score), 2)                       AS avg_review_score,
# MAGIC     COUNT(r.review_id)                                  AS review_count,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END) * 100.0
# MAGIC       / NULLIF(COUNT(r.review_id), 0),
# MAGIC       2
# MAGIC     )                                                   AS low_review_rate_pct
# MAGIC
# MAGIC   FROM olist_lakehouse_us.silver.sellers              s
# MAGIC   INNER JOIN olist_lakehouse_us.silver.order_items    oi ON s.seller_id = oi.seller_id
# MAGIC
# MAGIC   -- Inner-join the orders we want to scope to (delivered) - dedup via DISTINCT subquery
# MAGIC   INNER JOIN (
# MAGIC     SELECT DISTINCT order_id, delivery_days, delivery_delay_days, is_late_delivery
# MAGIC     FROM olist_lakehouse_us.silver.orders
# MAGIC     WHERE order_status = 'delivered'
# MAGIC   ) DISTINCT_orders ON oi.order_id = DISTINCT_orders.order_id
# MAGIC
# MAGIC   -- LEFT JOIN reviews (some orders have none)
# MAGIC   LEFT JOIN olist_lakehouse_us.silver.reviews r
# MAGIC     ON DISTINCT_orders.order_id = r.order_id
# MAGIC
# MAGIC   GROUP BY s.seller_id, s.seller_state, s.seller_city
# MAGIC   HAVING COUNT(DISTINCT oi.order_id) >= 5
# MAGIC ),
# MAGIC
# MAGIC -- Composite score using project plan's formula
# MAGIC seller_scored AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     -- Reviews component (40 pts max): treats no-review as 3-stars (no penalty)
# MAGIC     ROUND((COALESCE(avg_review_score, 3) / 5.0) * 40, 2)             AS review_score_component,
# MAGIC
# MAGIC     -- Delivery component (30 pts max): bucketed by avg delivery days
# MAGIC     CASE
# MAGIC       WHEN avg_delivery_days <=  7 THEN 30
# MAGIC       WHEN avg_delivery_days <= 14 THEN 20
# MAGIC       WHEN avg_delivery_days <= 21 THEN 10
# MAGIC       ELSE 0
# MAGIC     END                                                              AS delivery_score_component,
# MAGIC
# MAGIC     -- Volume component (30 pts max): saturates at 100 orders
# MAGIC     ROUND(LEAST(order_count / 100.0, 1.0) * 30, 2)                   AS volume_score_component
# MAGIC   FROM seller_metrics
# MAGIC ),
# MAGIC
# MAGIC -- Combine components into the composite score
# MAGIC seller_with_composite AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROUND(
# MAGIC       review_score_component + delivery_score_component + volume_score_component,
# MAGIC       2
# MAGIC     ) AS composite_score
# MAGIC   FROM seller_scored
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   seller_id,
# MAGIC   seller_state,
# MAGIC   seller_city,
# MAGIC
# MAGIC   -- Volume
# MAGIC   order_count,
# MAGIC   unique_products_sold,
# MAGIC   line_item_count,
# MAGIC
# MAGIC   -- Revenue
# MAGIC   product_revenue,
# MAGIC   freight_revenue,
# MAGIC   total_revenue,
# MAGIC   avg_item_price,
# MAGIC
# MAGIC   -- Delivery
# MAGIC   avg_delivery_days,
# MAGIC   avg_delay_days,
# MAGIC   late_rate_pct,
# MAGIC
# MAGIC   -- Reviews
# MAGIC   review_count,
# MAGIC   avg_review_score,
# MAGIC   low_review_rate_pct,
# MAGIC
# MAGIC   -- Composite components (transparent for "why is this seller's score X?" debugging)
# MAGIC   review_score_component,
# MAGIC   delivery_score_component,
# MAGIC   volume_score_component,
# MAGIC   composite_score,
# MAGIC
# MAGIC   -- Quintile tier (NTILE(5) on composite_score)
# MAGIC   -- Higher composite = higher quintile = better tier
# MAGIC   CASE NTILE(5) OVER (ORDER BY composite_score)
# MAGIC     WHEN 5 THEN 'top_20pct'
# MAGIC     WHEN 4 THEN 'second_20pct'
# MAGIC     WHEN 3 THEN 'middle_20pct'
# MAGIC     WHEN 2 THEN 'fourth_20pct'
# MAGIC     WHEN 1 THEN 'bottom_20pct'
# MAGIC   END AS performance_tier,
# MAGIC
# MAGIC   -- Lineage
# MAGIC   CURRENT_TIMESTAMP() AS _aggregated_at
# MAGIC
# MAGIC FROM seller_with_composite
# MAGIC ORDER BY composite_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC 1. **Row count** — should be substantially less than 3,095 (Phase 2's total seller count) because the min-5-orders threshold filters small sellers.
# MAGIC 2. **PK uniqueness** — `seller_id` is the PK.
# MAGIC 3. **Composite score bounds** — should be between 0 and 100 inclusive, no NULLs.
# MAGIC 4. **Component sum invariant** — `review_score_component + delivery_score_component + volume_score_component` should equal `composite_score` to within rounding (≤ 0.05).
# MAGIC 5. **Tier sizing invariant** — each performance tier should have ~20% of rows (NTILE(5) guarantees roughly equal buckets).
# MAGIC 6. **Top performer sanity** — top sellers by composite_score should have high review scores AND low delivery days AND ≥100 orders.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                AS total_sellers,
# MAGIC   COUNT(DISTINCT seller_id)                               AS distinct_sellers,
# MAGIC   ROUND(MIN(composite_score), 2)                          AS min_score,
# MAGIC   ROUND(MAX(composite_score), 2)                          AS max_score,
# MAGIC   COUNT(*) FILTER (
# MAGIC     WHERE composite_score < 0 OR composite_score > 100
# MAGIC   )                                                       AS rows_outside_bounds,
# MAGIC   COUNT(*) FILTER (
# MAGIC     WHERE ABS(
# MAGIC       review_score_component + delivery_score_component + volume_score_component
# MAGIC       - composite_score
# MAGIC     ) > 0.05
# MAGIC   )                                                       AS rows_violating_component_sum
# MAGIC FROM olist_lakehouse_us.gold.seller_scorecard;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   performance_tier,
# MAGIC   COUNT(*)                                  AS sellers,
# MAGIC   ROUND(MIN(composite_score), 2)            AS min_score_in_tier,
# MAGIC   ROUND(MAX(composite_score), 2)            AS max_score_in_tier,
# MAGIC   ROUND(AVG(composite_score), 2)            AS avg_score_in_tier,
# MAGIC   ROUND(AVG(order_count), 0)                AS avg_orders,
# MAGIC   ROUND(AVG(avg_review_score), 2)           AS avg_review_score,
# MAGIC   ROUND(AVG(avg_delivery_days), 1)          AS avg_delivery_days,
# MAGIC   ROUND(SUM(total_revenue), 0)              AS tier_total_revenue
# MAGIC FROM olist_lakehouse_us.gold.seller_scorecard
# MAGIC GROUP BY performance_tier
# MAGIC ORDER BY avg_score_in_tier DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC (SELECT 'top' AS rank_type, seller_id, seller_state, order_count, total_revenue,
# MAGIC         avg_review_score, avg_delivery_days, late_rate_pct, composite_score
# MAGIC  FROM olist_lakehouse_us.gold.seller_scorecard
# MAGIC  ORDER BY composite_score DESC
# MAGIC  LIMIT 10)
# MAGIC UNION ALL
# MAGIC (SELECT 'bottom', seller_id, seller_state, order_count, total_revenue,
# MAGIC         avg_review_score, avg_delivery_days, late_rate_pct, composite_score
# MAGIC  FROM olist_lakehouse_us.gold.seller_scorecard
# MAGIC  ORDER BY composite_score ASC
# MAGIC  LIMIT 10)
# MAGIC ORDER BY rank_type DESC, composite_score DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A nice cross-table query: which states have the best sellers on average?
# MAGIC -- Echoes Phase 2's "60% of sellers in SP" finding from a quality lens.
# MAGIC SELECT
# MAGIC   seller_state,
# MAGIC   COUNT(*)                              AS qualified_sellers,
# MAGIC   ROUND(AVG(composite_score), 2)        AS avg_composite_score,
# MAGIC   ROUND(SUM(total_revenue), 0)          AS state_total_revenue,
# MAGIC   ROUND(AVG(avg_review_score), 2)       AS avg_review_score_in_state
# MAGIC FROM olist_lakehouse_us.gold.seller_scorecard
# MAGIC GROUP BY seller_state
# MAGIC HAVING COUNT(*) >= 10  -- exclude micro-state samples
# MAGIC ORDER BY avg_composite_score DESC;

# COMMAND ----------

