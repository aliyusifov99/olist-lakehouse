# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Monthly Revenue
# MAGIC
# MAGIC **Layer:** Gold
# MAGIC **Source tables:** `silver.orders`, `silver.order_items`, `silver.products`
# MAGIC **Target table:** `olist_lakehouse_us.gold.monthly_revenue`
# MAGIC **Business question:** Q1 — What is total revenue by month/quarter/year, and how does it break down by product category?
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC Aggregates delivered orders to the **month × category** grain, producing one row
# MAGIC per `(month_start, category_name_en)` pair with revenue, order count, customer
# MAGIC count, and average item price.
# MAGIC
# MAGIC ## Grain & keys
# MAGIC
# MAGIC | Column | Role |
# MAGIC |---|---|
# MAGIC | `month_start` (DATE) | Time PK component — first day of the month |
# MAGIC | `category_name_en` (STRING) | Category PK component — English category name from `silver.products`; 'unknown' bucket retained |
# MAGIC | (`month_start`, `category_name_en`) | Composite primary key |
# MAGIC
# MAGIC ## Revenue definition
# MAGIC
# MAGIC `total_revenue = SUM(price + freight_value)` from `silver.order_items` — merchandise +
# MAGIC freight, **excludes** installment financing fees. Phase 2's reconciliation
# MAGIC audit found that only ~1% of orders have a meaningful items-vs-payments gap, and
# MAGIC the gap (~165K BRL on 15.84M BRL of revenue, +1.04%) is dominated by installment
# MAGIC surcharges that aren't merchandise revenue. The dashboard team can pull
# MAGIC `payment_value` separately from `gold.payment_analysis` when
# MAGIC financing-inclusive figures are needed.
# MAGIC
# MAGIC ## Filter rules
# MAGIC
# MAGIC - `order_status = 'delivered'` only — non-delivered orders aren't realized revenue.
# MAGIC - All categories included, including `category_name_en = 'unknown'` (the 610 products
# MAGIC   with missing source category data, per Phase 2). Reporting them in their own
# MAGIC   bucket keeps totals honest; the project plan's Q1 dashboard tile can filter
# MAGIC   them out at query time if needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why this shape
# MAGIC
# MAGIC **Composite PK at the finest useful grain.** Building at month × category is
# MAGIC ~1,850 rows max (74 categories × ~25 months in the dataset window). Rolling up to
# MAGIC month-only at dashboard read time is a trivial `GROUP BY` — building it the
# MAGIC other way would force per-category drill-downs back to Silver.
# MAGIC
# MAGIC **`month_start` as a DATE column.** Databricks SQL line charts plot DATE columns
# MAGIC directly without a `make_date(year, month, 1)` wrapper, and date-range filters are
# MAGIC clean (`WHERE month_start BETWEEN '2017-10-01' AND '2018-09-01'`). `order_year`,
# MAGIC `order_month`, and `order_quarter` are kept as helper columns so a dashboard can
# MAGIC group by them without re-extracting from the date.
# MAGIC
# MAGIC **Inner joins, not left joins.** Every metric in this table requires both an
# MAGIC order and at least one item. Orders without items wouldn't have revenue to
# MAGIC aggregate; items without orders shouldn't exist (FK enforced in the Phase 2
# MAGIC quality audit). INNER JOIN here is correct and lets the optimizer prune.
# MAGIC
# MAGIC **`COUNT(DISTINCT customer_id)` over `customer_unique_id`.** This table tracks
# MAGIC *purchase activity* (how many distinct buying sessions in the month), not *unique
# MAGIC people*. RFM-style person-level metrics belong in `customer_rfm`,
# MAGIC where `customer_unique_id` is the right grain.
# MAGIC
# MAGIC [CTAS reference](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop and recreate is the simplest pattern for a fully-aggregated Gold table.
# MAGIC -- Trade-off: loses Delta history vs INSERT OVERWRITE. Acceptable here because
# MAGIC -- this is a development rebuild; production refresh in Phase 5 will use a Job
# MAGIC -- with the same CTAS pattern but versioning is preserved at the workspace level.
# MAGIC
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.gold.monthly_revenue
# MAGIC USING DELTA
# MAGIC COMMENT 'Monthly revenue at month x category grain. Delivered orders only. Revenue = items.price + freight_value (excludes installment fees). PK: (month_start, category_name_en).'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'gold',
# MAGIC   'medallion.layer' = 'gold',
# MAGIC   'source.timezone' = 'America/Sao_Paulo'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   DATE_TRUNC('MONTH', o.order_purchase_ts)::DATE AS month_start,
# MAGIC   p.category_name_en,
# MAGIC
# MAGIC   -- Helper time columns for dashboard grouping convenience
# MAGIC   o.order_year,
# MAGIC   o.order_month,
# MAGIC   o.order_quarter,
# MAGIC
# MAGIC   -- Activity metrics
# MAGIC   COUNT(DISTINCT o.order_id)              AS order_count,
# MAGIC   COUNT(DISTINCT o.customer_id)           AS customer_count,
# MAGIC   COUNT(*)                                AS line_item_count,
# MAGIC
# MAGIC   -- Revenue metrics (BRL)
# MAGIC   ROUND(SUM(oi.price), 2)                 AS product_revenue,
# MAGIC   ROUND(SUM(oi.freight_value), 2)         AS freight_revenue,
# MAGIC   ROUND(SUM(oi.total_item_value), 2)      AS total_revenue,
# MAGIC
# MAGIC   -- Per-line-item averages
# MAGIC   ROUND(AVG(oi.price), 2)                 AS avg_item_price,
# MAGIC   ROUND(AVG(oi.freight_value), 2)         AS avg_freight_value,
# MAGIC
# MAGIC   -- Lineage
# MAGIC   CURRENT_TIMESTAMP()                     AS _aggregated_at
# MAGIC
# MAGIC FROM olist_lakehouse_us.silver.orders            o
# MAGIC INNER JOIN olist_lakehouse_us.silver.order_items oi ON o.order_id = oi.order_id
# MAGIC INNER JOIN olist_lakehouse_us.silver.products    p  ON oi.product_id = p.product_id
# MAGIC WHERE o.order_status = 'delivered'
# MAGIC GROUP BY
# MAGIC   DATE_TRUNC('MONTH', o.order_purchase_ts)::DATE,
# MAGIC   p.category_name_en,
# MAGIC   o.order_year,
# MAGIC   o.order_month,
# MAGIC   o.order_quarter
# MAGIC ORDER BY month_start, total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Four checks before we trust this table:
# MAGIC
# MAGIC 1. **Row count sanity** — should be in the low thousands (74 categories × ~25 months, sparse).
# MAGIC 2. **PK uniqueness** — no duplicate `(month_start, category_name_en)` pairs.
# MAGIC 3. **Total revenue reconciles to Phase 2 audit** — should match the 15.84M BRL
# MAGIC    total recorded, modulo the `delivered`-only filter
# MAGIC    (which excludes some line items from non-delivered orders).
# MAGIC 4. **'unknown' bucket present and bounded** — should appear in the table and have
# MAGIC    a meaningful but non-dominant share of revenue.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total Gold revenue should be slightly LOWER than Phase 2's 15.84M BRL
# MAGIC -- (which counted line items across all order statuses), because we filter to
# MAGIC -- delivered only. The delta is the value of items in canceled/unavailable/etc.
# MAGIC SELECT
# MAGIC   ROUND(SUM(total_revenue), 2)        AS gold_total_revenue_brl,
# MAGIC   SUM(order_count)                    AS gold_total_orders,
# MAGIC   ROUND(SUM(total_revenue) / SUM(order_count), 2) AS gold_avg_order_value
# MAGIC FROM olist_lakehouse_us.gold.monthly_revenue;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The 610 products with missing categories should appear here as 'unknown'
# MAGIC SELECT
# MAGIC   category_name_en,
# MAGIC   COUNT(*)                       AS month_buckets,
# MAGIC   ROUND(SUM(total_revenue), 2)   AS revenue_brl,
# MAGIC   ROUND(
# MAGIC     SUM(total_revenue) * 100.0 /
# MAGIC     SUM(SUM(total_revenue)) OVER (),
# MAGIC     2
# MAGIC   )                              AS pct_of_total
# MAGIC FROM olist_lakehouse_us.gold.monthly_revenue
# MAGIC WHERE category_name_en = 'unknown'
# MAGIC GROUP BY category_name_en;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick sanity check: project notes mention 'health_beauty'
# MAGIC -- as the top category. This query verifies that.
# MAGIC SELECT
# MAGIC   category_name_en,
# MAGIC   ROUND(SUM(total_revenue), 2)   AS revenue_brl,
# MAGIC   SUM(order_count)               AS orders
# MAGIC FROM olist_lakehouse_us.gold.monthly_revenue
# MAGIC GROUP BY category_name_en
# MAGIC ORDER BY revenue_brl DESC
# MAGIC LIMIT 10;

# COMMAND ----------

