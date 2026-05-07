# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Customer RFM Segmentation
# MAGIC
# MAGIC **Layer:** Gold
# MAGIC **Source tables:** `silver.orders`, `silver.order_items`, `silver.customers`
# MAGIC **Target table:** `olist_lakehouse_us.gold.customer_rfm`
# MAGIC **Business question:** Q3 — Who are the most valuable customers? What percentage of revenue comes from repeat vs. one-time buyers?
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC Computes Recency / Frequency / Monetary metrics for each unique customer
# MAGIC (`customer_unique_id`), bucketed into quintiles via `NTILE(5)`, and assigned to
# MAGIC named segments (Champions, Loyal Customers, At Risk, Lost, etc.) using
# MAGIC textbook RFM rules.
# MAGIC
# MAGIC ## Grain & keys
# MAGIC
# MAGIC | Column | Role |
# MAGIC |---|---|
# MAGIC | `customer_unique_id` (STRING) | Primary key — Phase 2's stable per-person identity column |
# MAGIC
# MAGIC This is the **first Gold table to use `customer_unique_id` as the grain**.
# MAGIC `silver.customers.customer_id` is per-order; rolling up to `customer_unique_id`
# MAGIC collapses the ~99K per-order customer rows to ~96K unique people.
# MAGIC
# MAGIC ## Reference date for Recency
# MAGIC
# MAGIC Recency is computed against the dataset's maximum `order_purchase_ts` — i.e.,
# MAGIC the latest date represented in the data (approximately 2018-10). Using
# MAGIC `CURRENT_DATE` would make every customer "ancient" (8 years old) and
# MAGIC collapse the recency distribution. Pinning to the dataset cutoff is
# MAGIC reproducible and standard practice for static historical analyses.
# MAGIC
# MAGIC ## Monetary definition
# MAGIC
# MAGIC `monetary = SUM(items.price + items.freight_value)` over the customer's
# MAGIC delivered orders — consistent with `gold.monthly_revenue`. Excludes installment
# MAGIC financing fees (those are visible in `gold.payment_analysis`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why this shape
# MAGIC
# MAGIC **`customer_unique_id` grain, not `customer_id`.** Phase 2's customer table
# MAGIC intentionally kept the per-order grain to preserve address-snapshot history.
# MAGIC RFM is a *person*-level analysis, so we aggregate up. The orders → customers
# MAGIC join uses `customer_id`; the final `GROUP BY` is on `customer_unique_id`.
# MAGIC
# MAGIC **Delivered orders only.** A canceled order isn't a "purchase" for monetary
# MAGIC or frequency purposes. Same filter rule as `gold.monthly_revenue`.
# MAGIC
# MAGIC **`NTILE(5)` quintile bucketing.** The textbook RFM scoring approach. Each
# MAGIC customer gets an R-score, F-score, and M-score from 1-5 based on their relative
# MAGIC position in the population. R is reverse-ordered (lower recency days = higher
# MAGIC R-score), F and M are forward-ordered (higher = higher score).
# MAGIC
# MAGIC ## ⚠ The Frequency-degeneracy caveat
# MAGIC
# MAGIC Phase 2's audit found **96.88% of customers buy exactly once**. That breaks
# MAGIC F-score in a structural way:
# MAGIC
# MAGIC - 93K of ~96K customers have `frequency = 1`
# MAGIC - `NTILE(5)` will spread these evenly across F-scores 1-5, but the assignment
# MAGIC   within ties is arbitrary (depends on row ordering)
# MAGIC - Two customers identical in every observable way may get different F-scores
# MAGIC   purely by NTILE's tiebreaker
# MAGIC
# MAGIC **This is not a bug in the SQL — it's a structural consequence of Olist's
# MAGIC marketplace model.** A real production RFM at Olist would use `DENSE_RANK`
# MAGIC on the value (so all F=1 customers share a score) and reweight segments toward
# MAGIC recency-and-monetary. We surface this with:
# MAGIC
# MAGIC - An `is_repeat_customer` boolean (true if frequency ≥ 2)
# MAGIC - A note on the table comment so consumers see the caveat in `DESCRIBE EXTENDED`
# MAGIC - Validation cells showing segment-size distribution
# MAGIC
# MAGIC ## Segment classification (textbook)
# MAGIC
# MAGIC | Segment | Rule | Interpretation |
# MAGIC |---|---|---|
# MAGIC | Champions | R≥4 AND F≥4 AND M≥4 | High recent spend + frequent buyers |
# MAGIC | Loyal Customers | R≥3 AND F≥3 AND M≥3 | Solid all-around performers |
# MAGIC | New Customers | R≥4 AND F≤2 | Recent but infrequent |
# MAGIC | At Risk | R≤2 AND F≥3 | Used to be frequent, gone quiet |
# MAGIC | Lost | R≤2 AND F≤2 AND M≤2 | Low everything, written off |
# MAGIC | Potential Loyalists | (default) | Anyone not matching the above |
# MAGIC
# MAGIC [NTILE function reference](https://docs.databricks.com/en/sql/language-manual/functions/ntile.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.gold.customer_rfm
# MAGIC USING DELTA
# MAGIC COMMENT 'Customer RFM segmentation at customer_unique_id grain. Reference date = dataset max(order_purchase_ts), approx 2018-10. NTILE(5) quintile scoring. WARNING: 97% of customers buy exactly once, so F-score is degenerate within the F=1 cluster (NTILE assigns scores arbitrarily within ties). Use is_repeat_customer flag for segment filtering when this matters.'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'gold',
# MAGIC   'medallion.layer' = 'gold',
# MAGIC   'source.timezone' = 'America/Sao_Paulo'
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC -- Reference date for recency calculation
# MAGIC reference_date AS (
# MAGIC   SELECT MAX(order_purchase_ts)::DATE AS ref_date
# MAGIC   FROM olist_lakehouse_us.silver.orders
# MAGIC   WHERE order_status = 'delivered'
# MAGIC ),
# MAGIC
# MAGIC -- Per-customer raw RFM metrics
# MAGIC customer_metrics AS (
# MAGIC   SELECT
# MAGIC     c.customer_unique_id,
# MAGIC
# MAGIC     -- Take the most-common state and city for this customer
# MAGIC     -- (per-order grain in silver.customers means a person can have multiple addresses)
# MAGIC     MODE(c.customer_state)            AS customer_state,
# MAGIC     MODE(c.customer_city)             AS customer_city,
# MAGIC
# MAGIC     -- R: days since last delivered order
# MAGIC     DATEDIFF(
# MAGIC       (SELECT ref_date FROM reference_date),
# MAGIC       MAX(o.order_purchase_ts)::DATE
# MAGIC     )                                 AS recency_days,
# MAGIC
# MAGIC     -- F: count of delivered orders (a person can have multiple per-order customer_id rows)
# MAGIC     COUNT(DISTINCT o.order_id)        AS frequency,
# MAGIC
# MAGIC     -- M: sum of items + freight across all their delivered orders
# MAGIC     ROUND(SUM(oi.total_item_value), 2) AS monetary,
# MAGIC
# MAGIC     -- Helpful auxiliary metrics
# MAGIC     MIN(o.order_purchase_ts)::DATE    AS first_order_date,
# MAGIC     MAX(o.order_purchase_ts)::DATE    AS last_order_date
# MAGIC
# MAGIC   FROM olist_lakehouse_us.silver.customers         c
# MAGIC   INNER JOIN olist_lakehouse_us.silver.orders      o  ON c.customer_id = o.customer_id
# MAGIC   INNER JOIN olist_lakehouse_us.silver.order_items oi ON o.order_id = oi.order_id
# MAGIC   WHERE o.order_status = 'delivered'
# MAGIC   GROUP BY c.customer_unique_id
# MAGIC ),
# MAGIC
# MAGIC -- NTILE(5) quintile scoring
# MAGIC -- R: reverse order (most recent = highest score)
# MAGIC -- F, M: forward order (highest value = highest score)
# MAGIC rfm_scored AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
# MAGIC     NTILE(5) OVER (ORDER BY frequency)         AS f_score,
# MAGIC     NTILE(5) OVER (ORDER BY monetary)          AS m_score,
# MAGIC     (frequency >= 2)                           AS is_repeat_customer
# MAGIC   FROM customer_metrics
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   customer_unique_id,
# MAGIC   customer_state,
# MAGIC   customer_city,
# MAGIC
# MAGIC   -- Raw RFM metrics
# MAGIC   recency_days,
# MAGIC   frequency,
# MAGIC   monetary,
# MAGIC   first_order_date,
# MAGIC   last_order_date,
# MAGIC
# MAGIC   -- Quintile scores
# MAGIC   r_score,
# MAGIC   f_score,
# MAGIC   m_score,
# MAGIC   CONCAT(r_score, f_score, m_score) AS rfm_combined,
# MAGIC   is_repeat_customer,
# MAGIC
# MAGIC   -- Textbook segment classification
# MAGIC   CASE
# MAGIC     WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
# MAGIC     WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
# MAGIC     WHEN r_score >= 4 AND f_score <= 2                  THEN 'New Customers'
# MAGIC     WHEN r_score <= 2 AND f_score >= 3                  THEN 'At Risk'
# MAGIC     WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Lost'
# MAGIC     ELSE 'Potential Loyalists'
# MAGIC   END AS customer_segment,
# MAGIC
# MAGIC   -- Lineage
# MAGIC   CURRENT_TIMESTAMP() AS _aggregated_at
# MAGIC
# MAGIC FROM rfm_scored;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC 1. **Row count** — should be ~96K, matching Phase 2's "96,096 distinct customer_unique_id" finding (modulo the delivered-only filter dropping a few).
# MAGIC 2. **PK uniqueness** — `customer_unique_id` is the PK.
# MAGIC 3. **Repeat-customer rate** — should be ~3.48% per Phase 2's audit.
# MAGIC 4. **Score distribution** — each NTILE bucket (1-5) should have roughly 20% of customers for R and M. F-score will be lumpy due to the 97% F=1 mass.
# MAGIC 5. **Segment distribution** — expect Lost and Potential Loyalists to dominate; Champions/Loyal should be a small minority.
# MAGIC 6. **Monetary reconciliation** — `SUM(monetary)` should approximately match `gold.monthly_revenue` total revenue (15.42M BRL), since both use items-source revenue on delivered orders.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                         AS total_customers,
# MAGIC   COUNT(DISTINCT customer_unique_id)               AS distinct_customer_ids,
# MAGIC   SUM(CASE WHEN is_repeat_customer THEN 1 END)     AS repeat_customers,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
# MAGIC     2
# MAGIC   )                                                AS repeat_customer_pct,
# MAGIC   MIN(recency_days)                                AS min_recency,
# MAGIC   MAX(recency_days)                                AS max_recency,
# MAGIC   MIN(frequency)                                   AS min_frequency,
# MAGIC   MAX(frequency)                                   AS max_frequency
# MAGIC FROM olist_lakehouse_us.gold.customer_rfm;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show how NTILE distributed F-score among the F=1 customers
# MAGIC -- This is the structural caveat made visible
# MAGIC SELECT
# MAGIC   frequency,
# MAGIC   COUNT(*)                       AS customers_at_this_freq,
# MAGIC   COUNT(DISTINCT f_score)        AS distinct_f_scores_assigned,
# MAGIC   MIN(f_score)                   AS min_f_score,
# MAGIC   MAX(f_score)                   AS max_f_score
# MAGIC FROM olist_lakehouse_us.gold.customer_rfm
# MAGIC GROUP BY frequency
# MAGIC ORDER BY frequency
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The headline output: how many customers in each named segment
# MAGIC SELECT
# MAGIC   customer_segment,
# MAGIC   COUNT(*)                                                              AS customer_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)                    AS pct_of_customers,
# MAGIC   ROUND(AVG(monetary), 2)                                               AS avg_monetary_brl,
# MAGIC   ROUND(SUM(monetary), 2)                                               AS total_monetary_brl,
# MAGIC   ROUND(SUM(monetary) * 100.0 / SUM(SUM(monetary)) OVER (), 2)          AS pct_of_revenue,
# MAGIC   ROUND(AVG(frequency), 2)                                              AS avg_frequency,
# MAGIC   ROUND(AVG(recency_days), 0)                                           AS avg_recency_days
# MAGIC FROM olist_lakehouse_us.gold.customer_rfm
# MAGIC GROUP BY customer_segment
# MAGIC ORDER BY total_monetary_brl DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Should match gold.monthly_revenue total (15.42M BRL ish)
# MAGIC SELECT
# MAGIC   ROUND(SUM(monetary), 2)        AS rfm_total_monetary_brl,
# MAGIC   COUNT(*)                       AS customers,
# MAGIC   ROUND(SUM(monetary) / COUNT(*), 2) AS avg_lifetime_value_brl
# MAGIC FROM olist_lakehouse_us.gold.customer_rfm;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Project plan asks: what percentage of revenue comes from repeat vs one-time buyers?
# MAGIC SELECT
# MAGIC   is_repeat_customer,
# MAGIC   COUNT(*)                                                    AS customers,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)          AS pct_of_customers,
# MAGIC   ROUND(SUM(monetary), 2)                                     AS total_revenue_brl,
# MAGIC   ROUND(SUM(monetary) * 100.0 / SUM(SUM(monetary)) OVER (), 2) AS pct_of_revenue,
# MAGIC   ROUND(AVG(monetary), 2)                                     AS avg_monetary_brl
# MAGIC FROM olist_lakehouse_us.gold.customer_rfm
# MAGIC GROUP BY is_repeat_customer
# MAGIC ORDER BY is_repeat_customer DESC;

# COMMAND ----------

