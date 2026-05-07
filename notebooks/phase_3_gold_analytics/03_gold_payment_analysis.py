# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Payment Analysis
# MAGIC
# MAGIC **Layer:** Gold
# MAGIC **Source tables:** `silver.payments`, `silver.orders`, `silver.order_items`
# MAGIC **Target table:** `olist_lakehouse_us.gold.payment_analysis`
# MAGIC **Business question:** Q6 — What is the distribution of payment types? How does installment count affect average order value?
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC Aggregates delivered orders to the **payment_type × installment_bucket** grain,
# MAGIC producing one row per `(payment_type, installment_bucket)` pair with payment
# MAGIC volume metrics, items-vs-payments reconciliation, and AOV statistics. This
# MAGIC table is the single Gold-layer home for items-vs-payments reconciliation
# MAGIC (deferred from `gold.monthly_revenue` in previous phase 3).
# MAGIC
# MAGIC ## Grain & keys
# MAGIC
# MAGIC | Column | Role |
# MAGIC |---|---|
# MAGIC | `payment_type` (STRING) | Payment instrument: credit_card / boleto / voucher / debit_card / not_defined |
# MAGIC | `installment_bucket` (STRING) | '1', '2-3', '4-6', '7-12', '13+' (per Phase 2) |
# MAGIC | (`payment_type`, `installment_bucket`) | Composite primary key |
# MAGIC
# MAGIC Sparse in practice: e.g., `boleto × 7-12` rows won't exist (boleto is single-payment).
# MAGIC
# MAGIC ## Filter rules
# MAGIC
# MAGIC - `order_status = 'delivered'` only (consistency with revenue-bearing tables)
# MAGIC - All payments included regardless of `payment_type_known` or `payment_value`
# MAGIC   status — the 12 anomalous rows from Phase 2's audit (3 `not_defined`,
# MAGIC   9 zero-value) are preserved. Dashboards can opt into clean filtering via
# MAGIC   `WHERE payment_type_known = true`.
# MAGIC
# MAGIC ## Reconciliation logic
# MAGIC
# MAGIC For each (payment_type, installment_bucket) slice:
# MAGIC - `total_payment_value` = SUM of all payment rows for orders in this slice
# MAGIC - `total_items_value` = SUM of all item rows (price + freight) for the same orders
# MAGIC - `payment_minus_items_gap` = `total_payment_value - total_items_value`
# MAGIC
# MAGIC A positive gap means installment fees / financing surcharges; a negative gap
# MAGIC means voucher coverage. Phase 2's audit found 1,004 positive-gap orders
# MAGIC (installment fees) vs 18 negative-gap (voucher), so most slices will show
# MAGIC positive gaps in the credit_card high-installment buckets.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why this shape
# MAGIC
# MAGIC **Dual-dimension grain.** 5 payment types × 5 installment buckets = up to 25
# MAGIC rows, sparse in practice. Single GROUP BY in SQL, lets dashboards slice either
# MAGIC direction by aggregating away the unwanted axis. Cheaper than maintaining two
# MAGIC separate tables.
# MAGIC
# MAGIC **Order-level aggregation before slicing.** Both `silver.payments` and
# MAGIC `silver.order_items` are at composite-PK grain (multi-row-per-order). Naive
# MAGIC join = Cartesian fanout. Fix: two parallel CTEs (`payments_per_order`,
# MAGIC `items_per_order`), each aggregated to one row per `order_id`, then joined
# MAGIC on `order_id`. This is the standard multi-fact reconciliation pattern.
# MAGIC
# MAGIC **Bucket attribution via the first payment row.** A multi-payment order (e.g.,
# MAGIC voucher + credit_card) has multiple payment_type values. We attribute the order
# MAGIC to the bucket of `payment_sequential = 1` — the *first* payment instrument used.
# MAGIC Voucher-then-credit-card orders bucket as voucher; credit-card-then-voucher
# MAGIC bucket as credit_card. The reconciliation totals are still correct (we sum
# MAGIC all payments for the order), but the bucket assignment is per-order-not-per-
# MAGIC payment.
# MAGIC
# MAGIC **`payment_type_known` flag computed at the order level, not aggregated.** A
# MAGIC boolean column on each row indicates whether the bucket is the clean
# MAGIC (non-`not_defined`) variant. This makes dashboard filtering trivial.
# MAGIC
# MAGIC [GROUP BY on multiple columns docs](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-groupby.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.gold.payment_analysis
# MAGIC USING DELTA
# MAGIC COMMENT 'Payment metrics by (payment_type, installment_bucket). Delivered orders only. Includes all payments (anomalies and zero-value preserved). Bucket attribution uses payment_sequential = 1 (first payment instrument). Reconciliation columns expose items-vs-payments gap per slice. PK: (payment_type, installment_bucket).'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'gold',
# MAGIC   'medallion.layer' = 'gold',
# MAGIC   'source.timezone' = 'America/Sao_Paulo'
# MAGIC )
# MAGIC AS
# MAGIC WITH
# MAGIC -- Per-order payment aggregation: one row per order_id
# MAGIC payments_per_order AS (
# MAGIC   SELECT
# MAGIC     p.order_id,
# MAGIC     SUM(p.payment_value)                                          AS order_total_payment,
# MAGIC     SUM(p.payment_installments)                                   AS order_total_installments_summed,
# MAGIC     COUNT(*)                                                      AS payment_row_count
# MAGIC
# MAGIC   FROM olist_lakehouse_us.silver.payments p
# MAGIC   GROUP BY p.order_id
# MAGIC ),
# MAGIC
# MAGIC -- Per-order items aggregation: one row per order_id
# MAGIC items_per_order AS (
# MAGIC   SELECT
# MAGIC     oi.order_id,
# MAGIC     SUM(oi.total_item_value)                                      AS order_total_items_value,
# MAGIC     COUNT(*)                                                      AS item_row_count
# MAGIC   FROM olist_lakehouse_us.silver.order_items oi
# MAGIC   GROUP BY oi.order_id
# MAGIC ),
# MAGIC
# MAGIC -- Per-order bucket assignment: pick the first payment row (payment_sequential = 1)
# MAGIC order_bucket AS (
# MAGIC   SELECT
# MAGIC     p.order_id,
# MAGIC     p.payment_type,
# MAGIC     p.payment_type_known,
# MAGIC     p.installment_bucket,
# MAGIC     p.payment_installments,
# MAGIC     p.installments_normalized
# MAGIC   FROM olist_lakehouse_us.silver.payments p
# MAGIC   WHERE p.payment_sequential = 1
# MAGIC ),
# MAGIC
# MAGIC -- Combine everything at the order level
# MAGIC order_level AS (
# MAGIC   SELECT
# MAGIC     ob.payment_type,
# MAGIC     ob.payment_type_known,
# MAGIC     ob.installment_bucket,
# MAGIC     ob.installments_normalized,
# MAGIC     o.order_id,
# MAGIC     ppo.order_total_payment,
# MAGIC     ipo.order_total_items_value,
# MAGIC     (ppo.order_total_payment - ipo.order_total_items_value) AS order_gap
# MAGIC   FROM order_bucket                ob
# MAGIC   INNER JOIN olist_lakehouse_us.silver.orders o   ON ob.order_id = o.order_id
# MAGIC   INNER JOIN payments_per_order   ppo ON ob.order_id = ppo.order_id
# MAGIC   LEFT  JOIN items_per_order      ipo ON ob.order_id = ipo.order_id
# MAGIC   WHERE o.order_status = 'delivered'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   payment_type,
# MAGIC   installment_bucket,
# MAGIC   payment_type_known,
# MAGIC
# MAGIC   -- Volume metrics
# MAGIC   COUNT(*)                                                       AS order_count,
# MAGIC   ROUND(AVG(installments_normalized), 2)                         AS avg_installments,
# MAGIC
# MAGIC   -- Payment-side totals
# MAGIC   ROUND(SUM(order_total_payment), 2)                             AS total_payment_value,
# MAGIC   ROUND(AVG(order_total_payment), 2)                             AS avg_payment_value,
# MAGIC   ROUND(PERCENTILE(order_total_payment, 0.5), 2)                 AS median_payment_value,
# MAGIC
# MAGIC   -- Items-side totals (LEFT JOIN means some orders may have NULL items if absent)
# MAGIC   ROUND(SUM(order_total_items_value), 2)                         AS total_items_value,
# MAGIC   ROUND(AVG(order_total_items_value), 2)                         AS avg_items_value,
# MAGIC
# MAGIC   -- Reconciliation
# MAGIC   ROUND(SUM(order_gap), 2)                                       AS payment_minus_items_gap,
# MAGIC   ROUND(AVG(order_gap), 2)                                       AS avg_gap_per_order,
# MAGIC   COUNT(*) FILTER (WHERE order_gap >  1.0)                       AS orders_with_positive_gap,
# MAGIC   COUNT(*) FILTER (WHERE order_gap < -1.0)                       AS orders_with_negative_gap,
# MAGIC
# MAGIC   -- Lineage
# MAGIC   CURRENT_TIMESTAMP()                                            AS _aggregated_at
# MAGIC
# MAGIC FROM order_level
# MAGIC GROUP BY payment_type, installment_bucket, payment_type_known
# MAGIC ORDER BY order_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC 1. **Row count** — should be ≤25 rows (5 × 5 grid, sparse in practice).
# MAGIC 2. **PK uniqueness** — `(payment_type, installment_bucket)` is the PK.
# MAGIC 3. **Total order count reconciles to delivered-orders population** — should be ~96K-97K (close to `gold.monthly_revenue` cell 6's 97,276), since each delivered order contributes exactly one row to `order_bucket` (the `payment_sequential = 1` row).
# MAGIC 4. **Total payment value reconciles to Phase 2's audit figure** — Phase 2 found 16,008,872.12 BRL total payment_value; this filtered to delivered should be slightly lower.
# MAGIC 5. **Total items value reconciles to `gold.monthly_revenue`** — should match 15,419,773.75 BRL.
# MAGIC 6. **Aggregate gap reconciles to Phase 2's installment-fee finding** — Phase 2 found ~165K BRL gap dominated by installment fees; `SUM(payment_minus_items_gap)` should be in that ballpark.
# MAGIC 7. **Boleto / debit_card single-installment dominance** — these payment types should overwhelmingly cluster in the `'1'` installment bucket.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                       AS total_rows,
# MAGIC   COUNT(DISTINCT payment_type, installment_bucket)               AS distinct_pk_combos,
# MAGIC   COUNT(DISTINCT payment_type)                                   AS distinct_payment_types,
# MAGIC   COUNT(DISTINCT installment_bucket)                             AS distinct_installment_buckets,
# MAGIC   SUM(order_count)                                               AS total_delivered_orders
# MAGIC FROM olist_lakehouse_us.gold.payment_analysis;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(SUM(total_payment_value), 2)                             AS gold_total_payment_brl,
# MAGIC   ROUND(SUM(total_items_value), 2)                               AS gold_total_items_brl,
# MAGIC   ROUND(SUM(payment_minus_items_gap), 2)                         AS gold_total_gap_brl,
# MAGIC   SUM(orders_with_positive_gap)                                  AS sum_orders_positive_gap,
# MAGIC   SUM(orders_with_negative_gap)                                  AS sum_orders_negative_gap
# MAGIC FROM olist_lakehouse_us.gold.payment_analysis;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Roll up over installment_bucket to get per-payment_type totals
# MAGIC SELECT
# MAGIC   payment_type,
# MAGIC   payment_type_known,
# MAGIC   SUM(order_count)                                               AS orders,
# MAGIC   ROUND(SUM(order_count) * 100.0 / SUM(SUM(order_count)) OVER (), 2)  AS pct_of_orders,
# MAGIC   ROUND(SUM(total_payment_value), 2)                             AS total_paid_brl,
# MAGIC   ROUND(SUM(total_payment_value) * 100.0 / SUM(SUM(total_payment_value)) OVER (), 2)  AS pct_of_revenue,
# MAGIC   ROUND(SUM(total_payment_value) / SUM(order_count), 2)          AS avg_order_value
# MAGIC FROM olist_lakehouse_us.gold.payment_analysis
# MAGIC GROUP BY payment_type, payment_type_known
# MAGIC ORDER BY orders DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Roll up over payment_type to get per-installment_bucket totals
# MAGIC -- Phase 2 found: avg 112 BRL (1) -> 134 (2-3) -> 181 (4-6) -> 333 (7-12) -> 414 (13+)
# MAGIC SELECT
# MAGIC   installment_bucket,
# MAGIC   SUM(order_count)                                               AS orders,
# MAGIC   ROUND(SUM(total_payment_value) / SUM(order_count), 2)          AS avg_order_value_brl,
# MAGIC   ROUND(SUM(total_payment_value), 2)                             AS total_paid_brl,
# MAGIC   ROUND(SUM(payment_minus_items_gap), 2)                         AS total_gap_brl,
# MAGIC   ROUND(SUM(payment_minus_items_gap) / SUM(total_payment_value) * 100.0, 2) AS gap_pct_of_payments
# MAGIC FROM olist_lakehouse_us.gold.payment_analysis
# MAGIC WHERE payment_type_known = TRUE
# MAGIC GROUP BY installment_bucket
# MAGIC ORDER BY
# MAGIC   CASE installment_bucket
# MAGIC     WHEN '1'    THEN 1
# MAGIC     WHEN '2-3'  THEN 2
# MAGIC     WHEN '4-6'  THEN 3
# MAGIC     WHEN '7-12' THEN 4
# MAGIC     WHEN '13+'  THEN 5
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which (payment_type, installment_bucket) slice has the highest AOV?
# MAGIC -- Likely credit_card 13+ — the "expensive electronics financed over 2 years" cohort
# MAGIC SELECT
# MAGIC   payment_type,
# MAGIC   installment_bucket,
# MAGIC   order_count,
# MAGIC   ROUND(avg_payment_value, 2)        AS avg_order_value_brl,
# MAGIC   ROUND(median_payment_value, 2)     AS median_order_value_brl,
# MAGIC   ROUND(payment_minus_items_gap, 2)  AS slice_total_gap_brl
# MAGIC FROM olist_lakehouse_us.gold.payment_analysis
# MAGIC WHERE payment_type_known = TRUE
# MAGIC   AND order_count >= 50
# MAGIC ORDER BY avg_payment_value DESC
# MAGIC LIMIT 10;

# COMMAND ----------

