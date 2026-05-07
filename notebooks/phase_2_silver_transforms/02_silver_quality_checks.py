# Databricks notebook source
# MAGIC %md
# MAGIC # `02_silver_quality_checks`
# MAGIC
# MAGIC **Cross-table data quality audit for the Silver layer.**
# MAGIC
# MAGIC This notebook asks the questions that can only be answered after every Silver table is built — primary key uniqueness, foreign key referential integrity, domain constraints, and cross-table reconciliation.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC - **Reports.** Every check returns a row count of violations.
# MAGIC - **Aggregates** all checks into a single results DataFrame at the end.
# MAGIC - **Does not fail** on violations. This is an audit; gating logic belongs in the Phase 4 DLT pipeline.
# MAGIC
# MAGIC ## Categories of checks
# MAGIC
# MAGIC 1. **PK contracts** — natural keys are unique and non-null on every Silver table.
# MAGIC 2. **FK referential integrity** — orphan rows across `orders → customers`, `order_items → orders/products/sellers`, `payments → orders`, `reviews → orders`.
# MAGIC 3. **Domain constraints** — enum values, value ranges, business rules.
# MAGIC 4. **Cross-table reconciliation** — `payment_value` vs `order_items` totals. Expected to be non-zero per Phase 1 (installment fees, vouchers, rounding); we quantify the gap.
# MAGIC 5. **Coverage gaps** — zip prefixes used by customers/sellers but missing from `silver.geolocation`. Re-reports the finding from previous steps.
# MAGIC
# MAGIC Per Phase 1, several "violations" are documented expected behavior:
# MAGIC - Reviews has 814 duplicate `review_id`s (composite PK is `(review_id, order_id)`).
# MAGIC - One order has no payment record.
# MAGIC - Some orders have no review.
# MAGIC - Payment totals differ from order_items totals.
# MAGIC
# MAGIC These show up in the report with status `INFO`, not `FAIL`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql import Row

CATALOG = "olist_lakehouse_us"
SCHEMA = "silver"

def run_check(check_name, sql, expected=0, status_kind="strict"):
    """
    Run a SQL check that returns a single row with one column.
    Coerces the result to float so mixed COUNT (LongType) and SUM (DoubleType)
    results merge into a single DataFrame schema cleanly.
    Returns: Row(check_name, expected, actual, status).
    """
    raw = spark.sql(sql).collect()[0][0]
    actual = float(raw) if raw is not None else 0.0
    if status_kind == "info":
        status = "INFO"
    else:
        status = "PASS" if actual == float(expected) else "FAIL"
    return Row(
        check_name=check_name,
        expected=float(expected),
        actual=actual,
        status=status,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Block 1 — Primary key contracts
# MAGIC
# MAGIC Each Silver table's natural key (composite or single) must be unique and non-null. We've checked these per-table during builds; consolidating here for the audit record.
# MAGIC
# MAGIC Reviews has 814 duplicate `review_id` values by design — its natural PK is `(review_id, order_id)`. Reported as `INFO` since it's documented behavior.

# COMMAND ----------

pk_checks = [
    # Single-column PKs: distinct count must equal row count and no nulls
    run_check(
        "orders.order_id is unique",
        f"SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM {CATALOG}.{SCHEMA}.orders"
    ),
    run_check(
        "orders.order_id has no nulls",
        f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.orders WHERE order_id IS NULL"
    ),
    run_check(
        "customers.customer_id is unique",
        f"SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM {CATALOG}.{SCHEMA}.customers"
    ),
    run_check(
        "products.product_id is unique",
        f"SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM {CATALOG}.{SCHEMA}.products"
    ),
    run_check(
        "sellers.seller_id is unique",
        f"SELECT COUNT(*) - COUNT(DISTINCT seller_id) FROM {CATALOG}.{SCHEMA}.sellers"
    ),
    run_check(
        "geolocation.zip_code_prefix is unique",
        f"SELECT COUNT(*) - COUNT(DISTINCT zip_code_prefix) FROM {CATALOG}.{SCHEMA}.geolocation"
    ),
    # Composite PKs
    run_check(
        "order_items (order_id, order_item_id) is unique",
        f"""SELECT COUNT(*) - COUNT(DISTINCT (order_id, order_item_id))
            FROM {CATALOG}.{SCHEMA}.order_items"""
    ),
    run_check(
        "payments (order_id, payment_sequential) is unique",
        f"""SELECT COUNT(*) - COUNT(DISTINCT (order_id, payment_sequential))
            FROM {CATALOG}.{SCHEMA}.payments"""
    ),
    run_check(
        "reviews (review_id, order_id) is unique",
        f"""SELECT COUNT(*) - COUNT(DISTINCT (review_id, order_id))
            FROM {CATALOG}.{SCHEMA}.reviews"""
    ),
    # Documented exception: review_id is NOT unique on its own
    run_check(
        "reviews.review_id duplicate count (expected ~814 per Phase 1)",
        f"""SELECT COUNT(*) - COUNT(DISTINCT review_id)
            FROM {CATALOG}.{SCHEMA}.reviews""",
        status_kind="info"
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Block 2 — Foreign key referential integrity
# MAGIC
# MAGIC Orphan checks: every FK value in a child table must exist in the parent. We use `LEFT JOIN ... WHERE parent.pk IS NULL` rather than `NOT IN` because `NOT IN` returns surprising results when the right side has any NULL (it returns nothing).
# MAGIC
# MAGIC Two known exceptions per Phase 1:
# MAGIC - One order has no payment row. So `orders → payments` is **not** a strict FK.
# MAGIC - Some orders have no review. So `orders → reviews` is **not** a strict FK.
# MAGIC
# MAGIC These are reported as INFO. The strict checks are in the child → parent direction (e.g., every `payments.order_id` must point to a real order).

# COMMAND ----------

fk_checks = [
    run_check(
        "orders.customer_id -> customers.customer_id (no orphans)",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.orders o
            LEFT JOIN {CATALOG}.{SCHEMA}.customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL"""
    ),
    run_check(
        "order_items.order_id -> orders.order_id (no orphans)",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.order_items oi
            LEFT JOIN {CATALOG}.{SCHEMA}.orders o ON oi.order_id = o.order_id
            WHERE o.order_id IS NULL"""
    ),
    run_check(
        "order_items.product_id -> products.product_id (no orphans)",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.order_items oi
            LEFT JOIN {CATALOG}.{SCHEMA}.products p ON oi.product_id = p.product_id
            WHERE p.product_id IS NULL"""
    ),
    run_check(
        "order_items.seller_id -> sellers.seller_id (no orphans)",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.order_items oi
            LEFT JOIN {CATALOG}.{SCHEMA}.sellers s ON oi.seller_id = s.seller_id
            WHERE s.seller_id IS NULL"""
    ),
    run_check(
        "payments.order_id -> orders.order_id (no orphans)",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.payments pm
            LEFT JOIN {CATALOG}.{SCHEMA}.orders o ON pm.order_id = o.order_id
            WHERE o.order_id IS NULL"""
    ),
    run_check(
        "reviews.order_id -> orders.order_id (no orphans)",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.reviews r
            LEFT JOIN {CATALOG}.{SCHEMA}.orders o ON r.order_id = o.order_id
            WHERE o.order_id IS NULL"""
    ),
    # Documented INFO exceptions: parent -> child gaps
    run_check(
        "orders without any payment row (expected ~1 per Phase 1)",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.orders o
            LEFT JOIN {CATALOG}.{SCHEMA}.payments pm ON o.order_id = pm.order_id
            WHERE pm.order_id IS NULL""",
        status_kind="info"
    ),
    run_check(
        "orders without any review (expected non-zero per Phase 1)",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.orders o
            LEFT JOIN {CATALOG}.{SCHEMA}.reviews r ON o.order_id = r.order_id
            WHERE r.order_id IS NULL""",
        status_kind="info"
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Block 3 — Domain constraints
# MAGIC
# MAGIC Value-range and enum checks. Most should pass; the ones that don't are findings worth surfacing.
# MAGIC
# MAGIC - `review_score` must be in `[1, 5]`.
# MAGIC - `order_status` must be in the known enum.
# MAGIC - `payment_value` should be `> 0` — but we already saw 9 zero-value rows in previous step and chose to surface them. Reported as INFO.
# MAGIC - `delivery_days` should be non-negative when present (delivered after purchased).
# MAGIC - `is_late_delivery` is a 3-valued boolean; total should reconcile to row count.

# COMMAND ----------

domain_checks = [
    run_check(
        "reviews.review_score in [1,5]",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.reviews
            WHERE review_score NOT BETWEEN 1 AND 5"""
    ),
    run_check(
        "orders.order_status in known enum",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.orders
            WHERE order_status NOT IN (
              'delivered','shipped','canceled','unavailable',
              'invoiced','processing','created','approved'
            )"""
    ),
    run_check(
        "order_items.price > 0",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.order_items WHERE price <= 0"""
    ),
    run_check(
        "order_items.freight_value >= 0",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.order_items WHERE freight_value < 0"""
    ),
    run_check(
        "orders.delivery_days >= 0 when delivered",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.orders
            WHERE delivery_days IS NOT NULL AND delivery_days < 0"""
    ),
    # INFO: zero-value payments
    run_check(
        "payments.payment_value <= 0",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.payments WHERE payment_value <= 0""",
        status_kind="info"
    ),
    # INFO: payment_type = 'not_defined' (3 rows expected)
    run_check(
        "payments with payment_type_known = false",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.payments
            WHERE payment_type_known = false""",
        status_kind="info"
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Block 4 — Cross-table reconciliation
# MAGIC
# MAGIC The interesting one. Phase 1 told us `sum(payment_value) per order ≠ sum(price + freight_value) per order` due to installment fees, vouchers, and rounding. We quantify the gap rather than alerting on it.
# MAGIC
# MAGIC Three angles:
# MAGIC
# MAGIC 1. **Total-level gap** — sum across all orders. Previous steps showed payments total ≈ 16.0M vs. order_items ≈ 15.84M (about 165K BRL gap, ~1%).
# MAGIC 2. **Per-order match rate** — what fraction of orders have payment ≈ items (within 1 BRL of rounding tolerance)?
# MAGIC 3. **Direction of mismatch** — are there more orders where payments exceed items (installment fees) vs. items exceed payments (vouchers covering the difference)?
# MAGIC
# MAGIC These are all `INFO` status — we expect non-zero values.

# COMMAND ----------

recon_checks = [
    run_check(
        "Total payment_value across all orders (BRL)",
        f"SELECT ROUND(SUM(payment_value), 2) FROM {CATALOG}.{SCHEMA}.payments",
        status_kind="info"
    ),
    run_check(
        "Total order_items value (price + freight) across all orders (BRL)",
        f"""SELECT ROUND(SUM(total_item_value), 2)
            FROM {CATALOG}.{SCHEMA}.order_items""",
        status_kind="info"
    ),
    run_check(
        "Orders where |payment_total - items_total| > 1 BRL",
        f"""WITH per_order AS (
              SELECT
                o.order_id,
                COALESCE((SELECT SUM(payment_value) FROM {CATALOG}.{SCHEMA}.payments p
                          WHERE p.order_id = o.order_id), 0) AS pay_total,
                COALESCE((SELECT SUM(total_item_value) FROM {CATALOG}.{SCHEMA}.order_items oi
                          WHERE oi.order_id = o.order_id), 0) AS item_total
              FROM {CATALOG}.{SCHEMA}.orders o
            )
            SELECT COUNT(*) FROM per_order
            WHERE ABS(pay_total - item_total) > 1.0""",
        status_kind="info"
    ),
    run_check(
        "Orders where payments exceed items by >1 BRL (installment fees / surcharges)",
        f"""WITH per_order AS (
              SELECT
                o.order_id,
                COALESCE((SELECT SUM(payment_value) FROM {CATALOG}.{SCHEMA}.payments p
                          WHERE p.order_id = o.order_id), 0) AS pay_total,
                COALESCE((SELECT SUM(total_item_value) FROM {CATALOG}.{SCHEMA}.order_items oi
                          WHERE oi.order_id = o.order_id), 0) AS item_total
              FROM {CATALOG}.{SCHEMA}.orders o
            )
            SELECT COUNT(*) FROM per_order
            WHERE pay_total - item_total > 1.0""",
        status_kind="info"
    ),
    run_check(
        "Orders where items exceed payments by >1 BRL (voucher / discount coverage)",
        f"""WITH per_order AS (
              SELECT
                o.order_id,
                COALESCE((SELECT SUM(payment_value) FROM {CATALOG}.{SCHEMA}.payments p
                          WHERE p.order_id = o.order_id), 0) AS pay_total,
                COALESCE((SELECT SUM(total_item_value) FROM {CATALOG}.{SCHEMA}.order_items oi
                          WHERE oi.order_id = o.order_id), 0) AS item_total
              FROM {CATALOG}.{SCHEMA}.orders o
            )
            SELECT COUNT(*) FROM per_order
            WHERE item_total - pay_total > 1.0""",
        status_kind="info"
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Block 5 — Coverage gaps
# MAGIC
# MAGIC Re-reports the geolocation coverage finding from previous steps: customer/seller zip prefixes that have no match in `silver.geolocation`. These will need state-centroid fallback in Gold to avoid silent data loss.

# COMMAND ----------

coverage_checks = [
    run_check(
        "Customer zip prefixes missing from silver.geolocation",
        f"""SELECT COUNT(DISTINCT c.customer_zip_code_prefix)
            FROM {CATALOG}.{SCHEMA}.customers c
            LEFT JOIN {CATALOG}.{SCHEMA}.geolocation g
              ON c.customer_zip_code_prefix = g.zip_code_prefix
            WHERE g.zip_code_prefix IS NULL""",
        status_kind="info"
    ),
    run_check(
        "Seller zip prefixes missing from silver.geolocation",
        f"""SELECT COUNT(DISTINCT s.seller_zip_code_prefix)
            FROM {CATALOG}.{SCHEMA}.sellers s
            LEFT JOIN {CATALOG}.{SCHEMA}.geolocation g
              ON s.seller_zip_code_prefix = g.zip_code_prefix
            WHERE g.zip_code_prefix IS NULL""",
        status_kind="info"
    ),
    run_check(
        "Products with category_name_en = 'unknown'",
        f"""SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.products
            WHERE category_name_en = 'unknown'""",
        status_kind="info"
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final report
# MAGIC
# MAGIC Aggregate every check into a single results DataFrame, sorted so failures (if any) bubble to the top.

# COMMAND ----------

all_checks = pk_checks + fk_checks + domain_checks + recon_checks + coverage_checks

# Explicit schema avoids any future type-inference surprises.
schema = StructType([
    StructField("check_name", StringType(), nullable=False),
    StructField("expected",   DoubleType(), nullable=False),
    StructField("actual",     DoubleType(), nullable=False),
    StructField("status",     StringType(), nullable=False),
])

results_df = (
    spark.createDataFrame(all_checks, schema=schema)
    # Order: FAIL first, then INFO, then PASS. Alphabetical within each.
    .withColumn(
        "_sort_status",
        F.when(F.col("status") == "FAIL", 0)
         .when(F.col("status") == "INFO", 1)
         .otherwise(2)
    )
    .orderBy("_sort_status", "check_name")
    .drop("_sort_status")
)

display(results_df)

print()
print("=" * 60)
print("Silver-layer quality audit summary:")
print("=" * 60)
results_df.groupBy("status").count().orderBy("status").show(truncate=False)

# COMMAND ----------

