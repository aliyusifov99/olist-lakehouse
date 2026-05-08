# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Delivery Performance
# MAGIC
# MAGIC **Layer:** Gold
# MAGIC **Source tables:** `silver.orders`, `silver.order_items`, `silver.sellers`
# MAGIC **UDF used:** `silver.delivery_sla_status`
# MAGIC **Target table:** `olist_lakehouse_us.gold.delivery_performance`
# MAGIC **Business question:** Q2 — What percentage of orders are delivered late? What is the average delivery time by seller state? Which shipping routes are most problematic?
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC Aggregates delivered orders to the **seller-state** grain, producing one row per
# MAGIC `seller_state` with delivery time metrics, SLA bucket distribution (wide format),
# MAGIC and freight statistics. A separate scalar metric (`non_delivery_rate`) is
# MAGIC computed from the full order population to surface the "stuck order" rate that
# MAGIC the delivered-only filter would otherwise hide.
# MAGIC
# MAGIC ## Grain & keys
# MAGIC
# MAGIC | Column | Role |
# MAGIC |---|---|
# MAGIC | `seller_state` (STRING) | Primary key — 2-char Brazilian state code |
# MAGIC
# MAGIC ## Filter rules
# MAGIC
# MAGIC - Primary aggregation: `order_status = 'delivered'` AND `delivery_delay_days IS NOT NULL`
# MAGIC   (these are equivalent in practice — Phase 2 confirmed delivery timestamps are
# MAGIC   populated for delivered orders only).
# MAGIC - `non_delivery_rate` metric uses the full order population (no status filter)
# MAGIC   to count orders that never reached the customer.
# MAGIC - Sellers with fewer than 10 delivered orders are excluded — small samples produce
# MAGIC   unstable percentage metrics. Project plan applies the same threshold.
# MAGIC
# MAGIC ## SLA buckets (from `silver.delivery_sla_status` UDF)
# MAGIC
# MAGIC | Bucket | Definition |
# MAGIC |---|---|
# MAGIC | `early` | Delivered 3+ days before estimate |
# MAGIC | `on_time` | Delivered within ±2 days of estimate |
# MAGIC | `slightly_late` | Delivered 3-7 days late |
# MAGIC | `very_late` | Delivered 8+ days late |
# MAGIC | `not_delivered` | `delivery_delay_days IS NULL` (excluded by filter for this table) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why this shape
# MAGIC
# MAGIC **Wide-format bucket counts.** Five columns (`early_count`, `on_time_count`,
# MAGIC `slightly_late_count`, `very_late_count`) directly drive Databricks SQL
# MAGIC stacked-bar widgets without a pivot step. Adding a `*_pct` column for each
# MAGIC bucket so the dashboard can show either absolute or relative breakdown without
# MAGIC re-computing.
# MAGIC
# MAGIC **Late rate definition matters.** Phase 2's audit found two numbers:
# MAGIC - **Strict late rate (8.1%)**: any delivery after estimate
# MAGIC - **SLA-bucket late rate (5.2%)**: only `slightly_late` + `very_late`, treats ±2 days as on-time
# MAGIC
# MAGIC We expose both. `is_late_delivery` from Silver gives the strict version
# MAGIC (`strict_late_rate`); the SLA UDF gives the grace-period version (`sla_late_rate`).
# MAGIC The project plan's "~6% late delivery rate" estimate aligns with the SLA version.
# MAGIC
# MAGIC **Non-delivery rate as a scalar metric.** Computed via a CTE from the full order
# MAGIC population (no `order_status = 'delivered'` filter), then joined to the per-state
# MAGIC metrics. This separates "delivery happened late" from "delivery never happened" —
# MAGIC two different operational problems.
# MAGIC
# MAGIC **Min-orders threshold (10).** A seller-state with only 2 orders shouldn't have
# MAGIC a "50% late rate" displayed alongside São Paulo's rate from 75K orders. Project
# MAGIC plan applies the same cutoff; we keep it.
# MAGIC
# MAGIC [delivery_sla_status UDF reference](https://docs.databricks.com/en/udf/unity-catalog.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.gold.delivery_performance
# MAGIC USING DELTA
# MAGIC COMMENT 'Delivery performance metrics by seller_state. Delivered orders only for SLA metrics. Non-delivery rate computed from full order population. Min 10 orders per state. PK: seller_state.'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'gold',
# MAGIC   'medallion.layer' = 'gold',
# MAGIC   'source.timezone' = 'America/Sao_Paulo'
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC -- Per-state non-delivery rate from the full order population (all statuses)
# MAGIC non_delivery_rates AS (
# MAGIC   SELECT
# MAGIC     s.seller_state,
# MAGIC     COUNT(DISTINCT o.order_id)                                                    AS total_orders_all_statuses,
# MAGIC     COUNT(DISTINCT CASE WHEN o.order_status != 'delivered' THEN o.order_id END)   AS non_delivered_orders,
# MAGIC     ROUND(
# MAGIC       COUNT(DISTINCT CASE WHEN o.order_status != 'delivered' THEN o.order_id END) * 100.0
# MAGIC       / NULLIF(COUNT(DISTINCT o.order_id), 0),
# MAGIC       2
# MAGIC     )                                                                             AS non_delivery_rate_pct
# MAGIC   FROM olist_lakehouse_us.silver.orders            o
# MAGIC   INNER JOIN olist_lakehouse_us.silver.order_items oi ON o.order_id = oi.order_id
# MAGIC   INNER JOIN olist_lakehouse_us.silver.sellers     s  ON oi.seller_id = s.seller_id
# MAGIC   GROUP BY s.seller_state
# MAGIC ),
# MAGIC
# MAGIC -- SLA bucket assignment at the order level (one bucket per order, not per line)
# MAGIC -- Pick the line-item's seller_state arbitrarily for orders that span multiple sellers
# MAGIC -- via FIRST_VALUE; this is acceptable because SLA is an order-level outcome.
# MAGIC order_level_sla AS (
# MAGIC   SELECT DISTINCT
# MAGIC     o.order_id,
# MAGIC     FIRST_VALUE(s.seller_state) OVER (PARTITION BY o.order_id ORDER BY oi.order_item_id) AS seller_state,
# MAGIC     o.delivery_days,
# MAGIC     o.delivery_delay_days,
# MAGIC     o.is_late_delivery,
# MAGIC     olist_lakehouse_us.silver.delivery_sla_status(o.delivery_delay_days) AS sla_bucket,
# MAGIC     -- Pick a representative freight for the order (sum of line-item freight)
# MAGIC     SUM(oi.freight_value) OVER (PARTITION BY o.order_id) AS order_freight_value
# MAGIC   FROM olist_lakehouse_us.silver.orders            o
# MAGIC   INNER JOIN olist_lakehouse_us.silver.order_items oi ON o.order_id = oi.order_id
# MAGIC   INNER JOIN olist_lakehouse_us.silver.sellers     s  ON oi.seller_id = s.seller_id
# MAGIC   WHERE o.order_status = 'delivered'
# MAGIC ),
# MAGIC
# MAGIC -- Per-state aggregation of delivered-order metrics
# MAGIC delivered_metrics AS (
# MAGIC   SELECT
# MAGIC     seller_state,
# MAGIC     COUNT(DISTINCT order_id)                                                  AS total_delivered_orders,
# MAGIC     ROUND(AVG(delivery_days), 1)                                              AS avg_delivery_days,
# MAGIC     ROUND(PERCENTILE(delivery_days, 0.5), 1)                                  AS median_delivery_days,
# MAGIC     ROUND(PERCENTILE(delivery_days, 0.9), 1)                                  AS p90_delivery_days,
# MAGIC     ROUND(AVG(delivery_delay_days), 1)                                        AS avg_delay_days,
# MAGIC
# MAGIC     -- Strict late rate (any delivery after estimate)
# MAGIC     ROUND(SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS strict_late_rate_pct,
# MAGIC
# MAGIC     -- SLA bucket counts (wide format)
# MAGIC     SUM(CASE WHEN sla_bucket = 'early'         THEN 1 ELSE 0 END) AS early_count,
# MAGIC     SUM(CASE WHEN sla_bucket = 'on_time'       THEN 1 ELSE 0 END) AS on_time_count,
# MAGIC     SUM(CASE WHEN sla_bucket = 'slightly_late' THEN 1 ELSE 0 END) AS slightly_late_count,
# MAGIC     SUM(CASE WHEN sla_bucket = 'very_late'     THEN 1 ELSE 0 END) AS very_late_count,
# MAGIC
# MAGIC     -- SLA bucket percentages (wide format) - sum to 100% per row
# MAGIC     ROUND(SUM(CASE WHEN sla_bucket = 'early'         THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS early_pct,
# MAGIC     ROUND(SUM(CASE WHEN sla_bucket = 'on_time'       THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS on_time_pct,
# MAGIC     ROUND(SUM(CASE WHEN sla_bucket = 'slightly_late' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS slightly_late_pct,
# MAGIC     ROUND(SUM(CASE WHEN sla_bucket = 'very_late'     THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS very_late_pct,
# MAGIC
# MAGIC     -- Composite SLA late rate (slightly_late + very_late) - the headline 6% number
# MAGIC     ROUND(
# MAGIC       (SUM(CASE WHEN sla_bucket = 'slightly_late' THEN 1 ELSE 0 END)
# MAGIC        + SUM(CASE WHEN sla_bucket = 'very_late' THEN 1 ELSE 0 END)) * 100.0 / COUNT(*),
# MAGIC       2
# MAGIC     ) AS sla_late_rate_pct,
# MAGIC
# MAGIC     -- Freight metrics
# MAGIC     ROUND(AVG(order_freight_value), 2)                                        AS avg_freight_brl,
# MAGIC     ROUND(PERCENTILE(order_freight_value, 0.5), 2)                            AS median_freight_brl
# MAGIC   FROM order_level_sla
# MAGIC   GROUP BY seller_state
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   d.seller_state,
# MAGIC   d.total_delivered_orders,
# MAGIC
# MAGIC   -- Delivery time metrics
# MAGIC   d.avg_delivery_days,
# MAGIC   d.median_delivery_days,
# MAGIC   d.p90_delivery_days,
# MAGIC   d.avg_delay_days,
# MAGIC
# MAGIC   -- Late rate metrics (two definitions for honesty)
# MAGIC   d.strict_late_rate_pct,
# MAGIC   d.sla_late_rate_pct,
# MAGIC
# MAGIC   -- SLA bucket distribution (wide format)
# MAGIC   d.early_count,         d.early_pct,
# MAGIC   d.on_time_count,       d.on_time_pct,
# MAGIC   d.slightly_late_count, d.slightly_late_pct,
# MAGIC   d.very_late_count,     d.very_late_pct,
# MAGIC
# MAGIC   -- Freight
# MAGIC   d.avg_freight_brl,
# MAGIC   d.median_freight_brl,
# MAGIC
# MAGIC   -- Non-delivery rate from the full order population
# MAGIC   n.total_orders_all_statuses,
# MAGIC   n.non_delivered_orders,
# MAGIC   n.non_delivery_rate_pct,
# MAGIC
# MAGIC   -- Lineage
# MAGIC   CURRENT_TIMESTAMP() AS _aggregated_at
# MAGIC
# MAGIC FROM delivered_metrics d
# MAGIC LEFT JOIN non_delivery_rates n ON d.seller_state = n.seller_state
# MAGIC WHERE d.total_delivered_orders >= 10
# MAGIC ORDER BY d.sla_late_rate_pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC 1. **Row count sanity** — should be ≤23 rows (Phase 2 found 23 of 27 states have sellers, minus any below the 10-order threshold).
# MAGIC 2. **PK uniqueness** — `seller_state` is the PK.
# MAGIC 3. **Bucket percentages sum to 100%** — `early_pct + on_time_pct + slightly_late_pct + very_late_pct` should equal 100 (±0.05 for rounding) on every row.
# MAGIC 4. **Late rate ordering invariant** — `strict_late_rate_pct >= sla_late_rate_pct` on every row (strict counts more rows as late by definition).
# MAGIC 5. **National-level reconciliation** — re-aggregating across all states should approximately match Phase 2's audit numbers (avg 12.5 delivery days, 5.2% SLA late rate, 8.1% strict late rate, 3.0% non-delivery).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                              AS total_rows,
# MAGIC   COUNT(DISTINCT seller_state)          AS distinct_states,
# MAGIC   -- The bucket percentage invariant: each row's buckets should sum to ~100%
# MAGIC   COUNT(*) FILTER (
# MAGIC     WHERE ABS((early_pct + on_time_pct + slightly_late_pct + very_late_pct) - 100) > 0.05
# MAGIC   ) AS rows_with_bucket_sum_violation,
# MAGIC   -- Late rate ordering invariant
# MAGIC   COUNT(*) FILTER (
# MAGIC     WHERE strict_late_rate_pct < sla_late_rate_pct
# MAGIC   ) AS rows_violating_late_rate_ordering
# MAGIC FROM olist_lakehouse_us.gold.delivery_performance;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Volume-weighted national averages should match Phase 2's audit numbers
# MAGIC SELECT
# MAGIC   ROUND(SUM(avg_delivery_days * total_delivered_orders) / SUM(total_delivered_orders), 1)     AS national_avg_delivery_days,
# MAGIC   ROUND(SUM(strict_late_rate_pct * total_delivered_orders) / SUM(total_delivered_orders), 2)  AS national_strict_late_pct,
# MAGIC   ROUND(SUM(sla_late_rate_pct * total_delivered_orders) / SUM(total_delivered_orders), 2)     AS national_sla_late_pct,
# MAGIC   ROUND(SUM(non_delivery_rate_pct * total_orders_all_statuses) / SUM(total_orders_all_statuses), 2)  AS national_non_delivery_pct,
# MAGIC   SUM(total_delivered_orders)                                                                 AS sum_delivered_orders
# MAGIC FROM olist_lakehouse_us.gold.delivery_performance;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The states most worth investigating
# MAGIC (SELECT 'worst' AS rank_type, seller_state, total_delivered_orders, avg_delivery_days, sla_late_rate_pct, non_delivery_rate_pct
# MAGIC  FROM olist_lakehouse_us.gold.delivery_performance
# MAGIC  ORDER BY sla_late_rate_pct DESC
# MAGIC  LIMIT 5)
# MAGIC UNION ALL
# MAGIC (SELECT 'best', seller_state, total_delivered_orders, avg_delivery_days, sla_late_rate_pct, non_delivery_rate_pct
# MAGIC  FROM olist_lakehouse_us.gold.delivery_performance
# MAGIC  ORDER BY sla_late_rate_pct ASC
# MAGIC  LIMIT 5)
# MAGIC ORDER BY rank_type, sla_late_rate_pct DESC;

# COMMAND ----------

