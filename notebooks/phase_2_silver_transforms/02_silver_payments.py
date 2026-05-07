# Databricks notebook source
# MAGIC %md
# MAGIC # `02_silver_payments`
# MAGIC
# MAGIC **Silver Layer:** payments with installment normalization and quality flags.
# MAGIC
# MAGIC - **Source:** `olist_lakehouse_us.bronze.payments` (103,886 rows; 4,446 duplicate `order_id`s expected — multi-instrument payments)
# MAGIC - **Target:** `olist_lakehouse_us.silver.payments`
# MAGIC
# MAGIC ## Transformations applied
# MAGIC
# MAGIC 1. Defensive type re-assertion on numeric columns.
# MAGIC 2. Add `payment_type_known` boolean (`FALSE` for `'not_defined'`).
# MAGIC 3. Add `installments_normalized` (0 → 1, otherwise verbatim).
# MAGIC 4. Add `installment_bucket` for downstream dashboards.
# MAGIC 5. Add `_processed_at` lineage column.
# MAGIC 6. Enforce composite PK `(order_id, payment_sequential)` — neither nullable.
# MAGIC
# MAGIC ## What we deliberately don't do
# MAGIC
# MAGIC - **Don't filter `payment_type = 'not_defined'`.** It's real data signaling a source gap. Surfaced via the `payment_type_known` boolean.
# MAGIC - **Don't fix `payment_installments = 0`.** Kept verbatim for audit; `installments_normalized` provides the cleaned version for arithmetic.
# MAGIC - **Don't reconcile to `order_items` totals.** `payment_value ≠ sum(price + freight_value)` due to installment fees, vouchers, and rounding. That cross-table check belongs in the dedicated quality notebook.
# MAGIC
# MAGIC ## Composite key
# MAGIC
# MAGIC A single order can split across multiple payment instruments — voucher + credit card is a common pattern. 4,446 `order_id` duplicates are expected. PK is `(order_id, payment_sequential)`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.silver.payments
# MAGIC USING DELTA
# MAGIC COMMENT 'Cleaned payments with installment normalization and quality flags. '
# MAGIC         'PK is composite (order_id, payment_sequential). payment_value does NOT '
# MAGIC         'reconcile to sum(order_items.price + freight_value) due to installment '
# MAGIC         'fees, vouchers, and rounding.'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'silver',
# MAGIC   'medallion.layer' = 'silver'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- Composite PK
# MAGIC   order_id,
# MAGIC   CAST(payment_sequential AS INT) AS payment_sequential,
# MAGIC
# MAGIC   -- Raw fields, kept verbatim for audit
# MAGIC   payment_type,
# MAGIC   CAST(payment_installments AS INT) AS payment_installments,
# MAGIC   CAST(payment_value AS DOUBLE) AS payment_value,
# MAGIC
# MAGIC   -- Derived: is the payment type known? FALSE for 'not_defined' rows.
# MAGIC   (payment_type IS NOT NULL AND payment_type <> 'not_defined') AS payment_type_known,
# MAGIC
# MAGIC   -- Derived: installments normalized for arithmetic. 0 -> 1 (treating zero as single payment).
# MAGIC   CASE
# MAGIC     WHEN payment_installments IS NULL OR payment_installments = 0 THEN 1
# MAGIC     ELSE CAST(payment_installments AS INT)
# MAGIC   END AS installments_normalized,
# MAGIC
# MAGIC   -- Derived: installment bucket for dashboards
# MAGIC   CASE
# MAGIC     WHEN payment_installments IS NULL OR payment_installments <= 1 THEN '1'
# MAGIC     WHEN payment_installments BETWEEN 2 AND 3  THEN '2-3'
# MAGIC     WHEN payment_installments BETWEEN 4 AND 6  THEN '4-6'
# MAGIC     WHEN payment_installments BETWEEN 7 AND 12 THEN '7-12'
# MAGIC     ELSE '13+'
# MAGIC   END AS installment_bucket,
# MAGIC
# MAGIC   -- Lineage
# MAGIC   _ingested_at,
# MAGIC   CURRENT_TIMESTAMP() AS _processed_at
# MAGIC
# MAGIC FROM olist_lakehouse_us.bronze.payments
# MAGIC WHERE order_id IS NOT NULL
# MAGIC   AND payment_sequential IS NOT NULL;  -- Silver PK contract

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Four checks:
# MAGIC
# MAGIC 1. **Structural** — row count, composite-key uniqueness, distinct payment types and orders.
# MAGIC 2. **Business rules** — payment_value > 0, no NULL PK components.
# MAGIC 3. **Payment type distribution** — confirms the 5-value enum and quantifies `not_defined`.
# MAGIC 4. **Installment distribution** — confirms the bucket logic against raw values.
# MAGIC
# MAGIC ### Expected values
# MAGIC
# MAGIC | Metric | Expected |
# MAGIC |---|---|
# MAGIC | `total_rows` | 103,886 |
# MAGIC | `distinct_pk_pairs` | 103,886 (composite key is unique) |
# MAGIC | `distinct_orders` | 99,440 (= 103,886 − 4,446) |
# MAGIC | `distinct_payment_types` | ≤5 |
# MAGIC | `not_defined_rows` | small but nonzero (per Phase 1) |
# MAGIC | `zero_installment_rows` | small but nonzero (per Phase 1) |
# MAGIC | `nonpositive_payment_value` | 0 |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                  AS total_rows,
# MAGIC   COUNT(DISTINCT (order_id, payment_sequential))            AS distinct_pk_pairs,
# MAGIC   COUNT(DISTINCT order_id)                                  AS distinct_orders,
# MAGIC   COUNT(DISTINCT payment_type)                              AS distinct_payment_types,
# MAGIC   SUM(CASE WHEN NOT payment_type_known THEN 1 ELSE 0 END)   AS not_defined_rows,
# MAGIC   SUM(CASE WHEN payment_installments = 0 THEN 1 ELSE 0 END) AS zero_installment_rows,
# MAGIC   SUM(CASE WHEN payment_value <= 0 THEN 1 ELSE 0 END)       AS nonpositive_payment_value,
# MAGIC   SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)         AS null_order_id,
# MAGIC   ROUND(SUM(payment_value), 2)                              AS total_paid_brl,
# MAGIC   ROUND(AVG(payment_value), 2)                              AS avg_payment_value
# MAGIC FROM olist_lakehouse_us.silver.payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   payment_type,
# MAGIC   COUNT(*)                                                AS payment_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)      AS pct,
# MAGIC   ROUND(SUM(payment_value), 2)                            AS total_value_brl,
# MAGIC   ROUND(AVG(payment_value), 2)                            AS avg_value_brl
# MAGIC FROM olist_lakehouse_us.silver.payments
# MAGIC GROUP BY payment_type
# MAGIC ORDER BY payment_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   installment_bucket,
# MAGIC   COUNT(*)                                                AS payment_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)      AS pct,
# MAGIC   MIN(payment_installments)                               AS min_raw_installments,
# MAGIC   MAX(payment_installments)                               AS max_raw_installments,
# MAGIC   ROUND(AVG(payment_value), 2)                            AS avg_payment_value
# MAGIC FROM olist_lakehouse_us.silver.payments
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
# MAGIC -- Investigate the 9 nonpositive payment_value rows.
# MAGIC -- Are they refunds? Voids? The 3 'not_defined' anomalies? Spread across order_ids?
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   payment_sequential,
# MAGIC   payment_type,
# MAGIC   payment_installments,
# MAGIC   payment_value
# MAGIC FROM olist_lakehouse_us.silver.payments
# MAGIC WHERE payment_value <= 0
# MAGIC ORDER BY payment_value, order_id;

# COMMAND ----------

