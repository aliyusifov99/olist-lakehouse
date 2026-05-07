# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Geographic Metrics
# MAGIC
# MAGIC **Layer:** Gold
# MAGIC **Source tables:** `silver.orders`, `silver.order_items`, `silver.customers`, `silver.sellers`
# MAGIC **Target table:** `olist_lakehouse_us.gold.geographic_metrics`
# MAGIC **Business question:** Q7 — Which states generate the most revenue? What is the average freight cost by customer-seller state pair? Where are the logistics bottlenecks?
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC Aggregates delivered orders to the **`(customer_state, seller_state)` route grain**,
# MAGIC producing one row per shipping route with revenue, freight, delivery time, and
# MAGIC volume metrics. Up to ~27×27 = 729 cells, sparse in practice (many routes have
# MAGIC zero orders). Routes with fewer than 5 orders are excluded.
# MAGIC
# MAGIC ## Grain & keys
# MAGIC
# MAGIC | Column | Role |
# MAGIC |---|---|
# MAGIC | `customer_state` (STRING) | Buyer's state — 2-char Brazilian state code |
# MAGIC | `seller_state` (STRING) | Seller's state — 2-char Brazilian state code |
# MAGIC | (`customer_state`, `seller_state`) | Composite primary key |
# MAGIC
# MAGIC ## Filter rules
# MAGIC
# MAGIC - `order_status = 'delivered'` only (consistency with all revenue-bearing Gold tables).
# MAGIC - Routes with fewer than 5 delivered orders are dropped — same threshold as
# MAGIC   `gold.seller_scorecard`. Stabilizes per-route freight/delivery averages.
# MAGIC - No `silver.geolocation` join — `customer_state` and `seller_state` come
# MAGIC   directly from `silver.customers` and `silver.sellers`. Geolocation lat/lng
# MAGIC   centroids are not used in this table; they remain available for future
# MAGIC   map-rendering or distance-correlation analyses.
# MAGIC
# MAGIC ## Multi-seller orders
# MAGIC
# MAGIC Each `order_items` row has exactly one `seller_id`, so seller_state
# MAGIC attribution is per-line-item. An order shipped from 2 sellers in 2 states
# MAGIC will appear in 2 different routes (one row per seller's state). This is
# MAGIC correct: from a logistics perspective, each line item is a separate shipment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why this shape
# MAGIC
# MAGIC **Route grain (customer_state × seller_state).** The same dual-grain pattern
# MAGIC we used in `gold.payment_analysis`. Lets dashboards roll up either axis
# MAGIC (`GROUP BY customer_state` for buyer-side analysis, `GROUP BY seller_state`
# MAGIC for supply-side). The cross product captures freight and delivery variance
# MAGIC that pure single-state aggregation misses — SP→AC freight is 5× SP→RJ freight,
# MAGIC but a "by seller_state" rollup hides this completely.
# MAGIC
# MAGIC **Per-line-item attribution, not per-order.** Each `order_items` row has one
# MAGIC seller, so an order spanning 2 sellers contributes 2 line-item rows to 2
# MAGIC different routes. Revenue and freight aggregate naturally; order_count uses
# MAGIC `COUNT(DISTINCT order_id)` per route to avoid line-item inflation.
# MAGIC
# MAGIC **Volume threshold via `HAVING`, not `is_significant_route` flag.** Routes
# MAGIC below 5 orders are dropped entirely, not flagged. This matches
# MAGIC `gold.seller_scorecard`'s pattern — a hard cutoff is appropriate when
# MAGIC metrics are unstable below the threshold. The flag approach in
# MAGIC `seller_scorecard`'s tier sizing was about something different
# MAGIC (quintile assignment), not stability.
# MAGIC
# MAGIC **Both endpoints' perspective stored as denormalized columns.** Each row
# MAGIC includes `customer_state`, `seller_state`, AND a derived `is_intra_state`
# MAGIC boolean (true if customer_state = seller_state). This makes
# MAGIC "intra-state vs cross-state" a one-column filter at the dashboard layer
# MAGIC rather than a CASE expression every query.
# MAGIC
# MAGIC [GROUP BY composite key reference](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-groupby.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.gold.geographic_metrics
# MAGIC USING DELTA
# MAGIC COMMENT 'Per-route revenue, freight, and delivery metrics. Route = (customer_state, seller_state). Delivered orders only. Min 5 orders per route. Multi-seller orders contribute one row per seller-state. PK: (customer_state, seller_state).'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'gold',
# MAGIC   'medallion.layer' = 'gold',
# MAGIC   'source.timezone' = 'America/Sao_Paulo'
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC -- Order × seller_state pairs with delivery metrics carried through
# MAGIC -- Each line item adds a (order_id, seller_state) pair; same order with multiple
# MAGIC -- seller_states yields multiple pairs.
# MAGIC order_route_pairs AS (
# MAGIC   SELECT
# MAGIC     o.order_id,
# MAGIC     o.delivery_days,
# MAGIC     o.delivery_delay_days,
# MAGIC     o.is_late_delivery,
# MAGIC     c.customer_state,
# MAGIC     s.seller_state,
# MAGIC     oi.price,
# MAGIC     oi.freight_value,
# MAGIC     oi.total_item_value
# MAGIC   FROM olist_lakehouse_us.silver.orders            o
# MAGIC   INNER JOIN olist_lakehouse_us.silver.customers   c  ON o.customer_id = c.customer_id
# MAGIC   INNER JOIN olist_lakehouse_us.silver.order_items oi ON o.order_id    = oi.order_id
# MAGIC   INNER JOIN olist_lakehouse_us.silver.sellers     s  ON oi.seller_id  = s.seller_id
# MAGIC   WHERE o.order_status = 'delivered'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   customer_state,
# MAGIC   seller_state,
# MAGIC   (customer_state = seller_state)                                              AS is_intra_state,
# MAGIC
# MAGIC   -- Volume
# MAGIC   COUNT(DISTINCT order_id)                                                     AS order_count,
# MAGIC   COUNT(*)                                                                     AS line_item_count,
# MAGIC
# MAGIC   -- Revenue (BRL)
# MAGIC   ROUND(SUM(price), 2)                                                         AS product_revenue,
# MAGIC   ROUND(SUM(freight_value), 2)                                                 AS freight_revenue,
# MAGIC   ROUND(SUM(total_item_value), 2)                                              AS total_revenue,
# MAGIC   ROUND(AVG(price), 2)                                                         AS avg_item_price,
# MAGIC   ROUND(AVG(freight_value), 2)                                                 AS avg_freight_value,
# MAGIC
# MAGIC   -- Freight as a percentage of merchandise (the headline freight metric)
# MAGIC   ROUND(SUM(freight_value) * 100.0 / NULLIF(SUM(price), 0), 2)                 AS freight_pct_of_price,
# MAGIC
# MAGIC   -- Delivery time metrics (DISTINCT order-level: avoid line-item duplication)
# MAGIC   ROUND(AVG(delivery_days), 1)                                                 AS avg_delivery_days,
# MAGIC   ROUND(PERCENTILE(delivery_days, 0.5), 1)                                     AS median_delivery_days,
# MAGIC   ROUND(PERCENTILE(delivery_days, 0.9), 1)                                     AS p90_delivery_days,
# MAGIC
# MAGIC   -- Late-rate metrics (strict definition, consistent with delivery_performance)
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) * 100.0
# MAGIC     / NULLIF(SUM(CASE WHEN is_late_delivery IS NOT NULL THEN 1 ELSE 0 END), 0),
# MAGIC     2
# MAGIC   )                                                                            AS strict_late_rate_pct,
# MAGIC
# MAGIC   -- Lineage
# MAGIC   CURRENT_TIMESTAMP()                                                          AS _aggregated_at
# MAGIC
# MAGIC FROM order_route_pairs
# MAGIC GROUP BY customer_state, seller_state
# MAGIC HAVING COUNT(DISTINCT order_id) >= 5
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC 1. **Row count** — should be in the 100-300 range. 27×27=729 max, but most exotic routes (small-state to small-state) won't hit the 5-order threshold.
# MAGIC 2. **PK uniqueness** — `(customer_state, seller_state)` is the PK.
# MAGIC 3. **Total revenue reconciliation with Phase 3 norms** — `SUM(total_revenue)` on this table will exceed `gold.monthly_revenue` total, because multi-seller orders contribute multiple rows here. Use `SUM(product_revenue)` from items, but expect line-item fanout.
# MAGIC 4. **Intra-state proportion sanity** — most orders are likely SP→SP (Phase 2 found 60% of sellers in SP, 42% of customers), so SP→SP should be the largest single route.
# MAGIC 5. **Freight-pct-of-price gradient** — distant routes (e.g., SP→AM) should show much higher `freight_pct_of_price` than intra-state (SP→SP). This is the sanity check for the freight metric.
# MAGIC 6. **Cross-state vs intra-state delivery time** — intra-state should average ~7-10 days; cross-state should average 12-18+ days.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                AS total_routes,
# MAGIC   COUNT(DISTINCT customer_state, seller_state)            AS distinct_pk_combos,
# MAGIC   COUNT(DISTINCT customer_state)                          AS distinct_customer_states,
# MAGIC   COUNT(DISTINCT seller_state)                            AS distinct_seller_states,
# MAGIC   SUM(CASE WHEN is_intra_state THEN 1 ELSE 0 END)         AS intra_state_routes,
# MAGIC   SUM(CASE WHEN NOT is_intra_state THEN 1 ELSE 0 END)     AS cross_state_routes
# MAGIC FROM olist_lakehouse_us.gold.geographic_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The biggest shipping lanes
# MAGIC SELECT
# MAGIC   customer_state,
# MAGIC   seller_state,
# MAGIC   is_intra_state,
# MAGIC   order_count,
# MAGIC   total_revenue,
# MAGIC   avg_freight_value,
# MAGIC   freight_pct_of_price,
# MAGIC   avg_delivery_days,
# MAGIC   strict_late_rate_pct
# MAGIC FROM olist_lakehouse_us.gold.geographic_metrics
# MAGIC ORDER BY order_count DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cheap-vs-expensive freight gradient: which routes pay the most for shipping?
# MAGIC SELECT
# MAGIC   customer_state,
# MAGIC   seller_state,
# MAGIC   is_intra_state,
# MAGIC   order_count,
# MAGIC   avg_freight_value,
# MAGIC   freight_pct_of_price,
# MAGIC   avg_delivery_days
# MAGIC FROM olist_lakehouse_us.gold.geographic_metrics
# MAGIC WHERE order_count >= 50  -- focus on routes with stable averages
# MAGIC ORDER BY freight_pct_of_price DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cheapest freight (likely all intra-state)
# MAGIC SELECT
# MAGIC   customer_state,
# MAGIC   seller_state,
# MAGIC   is_intra_state,
# MAGIC   order_count,
# MAGIC   avg_freight_value,
# MAGIC   freight_pct_of_price,
# MAGIC   avg_delivery_days
# MAGIC FROM olist_lakehouse_us.gold.geographic_metrics
# MAGIC WHERE order_count >= 50
# MAGIC ORDER BY freight_pct_of_price ASC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The headline contrast: how much more does cross-state shipping cost?
# MAGIC SELECT
# MAGIC   is_intra_state,
# MAGIC   COUNT(*)                                          AS routes,
# MAGIC   SUM(order_count)                                  AS orders,
# MAGIC   ROUND(SUM(total_revenue), 0)                      AS total_revenue_brl,
# MAGIC   ROUND(SUM(freight_revenue), 0)                    AS total_freight_brl,
# MAGIC   ROUND(AVG(avg_freight_value), 2)                  AS avg_freight_per_order,
# MAGIC   ROUND(AVG(freight_pct_of_price), 2)               AS avg_freight_pct,
# MAGIC   ROUND(AVG(avg_delivery_days), 1)                  AS avg_delivery_days,
# MAGIC   ROUND(AVG(strict_late_rate_pct), 2)               AS avg_late_rate
# MAGIC FROM olist_lakehouse_us.gold.geographic_metrics
# MAGIC GROUP BY is_intra_state
# MAGIC ORDER BY is_intra_state DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Phase 2 noted RJ has 13% of customers but only 5.5% of sellers (net-importer).
# MAGIC -- Quantify this: for each state, total revenue when buyer = state vs seller = state
# MAGIC WITH
# MAGIC buyer_side AS (
# MAGIC   SELECT customer_state AS state, SUM(total_revenue) AS revenue_in
# MAGIC   FROM olist_lakehouse_us.gold.geographic_metrics
# MAGIC   GROUP BY customer_state
# MAGIC ),
# MAGIC seller_side AS (
# MAGIC   SELECT seller_state AS state, SUM(total_revenue) AS revenue_out
# MAGIC   FROM olist_lakehouse_us.gold.geographic_metrics
# MAGIC   GROUP BY seller_state
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(b.state, s.state)                              AS state,
# MAGIC   ROUND(COALESCE(b.revenue_in, 0), 0)                     AS state_buyer_revenue_brl,
# MAGIC   ROUND(COALESCE(s.revenue_out, 0), 0)                    AS state_seller_revenue_brl,
# MAGIC   ROUND(COALESCE(s.revenue_out, 0) - COALESCE(b.revenue_in, 0), 0) AS net_export_brl,
# MAGIC   ROUND(
# MAGIC     COALESCE(s.revenue_out, 0) / NULLIF(COALESCE(b.revenue_in, 0), 0),
# MAGIC     2
# MAGIC   )                                                       AS export_to_import_ratio
# MAGIC FROM buyer_side b
# MAGIC FULL OUTER JOIN seller_side s ON b.state = s.state
# MAGIC ORDER BY net_export_brl DESC;

# COMMAND ----------

