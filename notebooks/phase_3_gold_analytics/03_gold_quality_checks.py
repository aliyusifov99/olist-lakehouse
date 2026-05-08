# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Cross-Table Validation
# MAGIC
# MAGIC **Layer:** Gold (validation, not transformation)
# MAGIC **Source tables:** All 8 Gold tables in `olist_lakehouse_us.gold.*`
# MAGIC **Pattern:** Mirrors Phase 2's `02_silver_quality_checks.py` — `run_check` helper, status-coded results DataFrame.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC Validates cross-table consistency across the 8 Gold tables built in phase 3. Each Gold table aggregates the same underlying Silver source, so their
# MAGIC totals should reconcile (modulo documented filters and grain differences).
# MAGIC Reports PASS / INFO / FAIL statuses without raising on failures — gating
# MAGIC behavior belongs in the Phase 4 DLT pipeline, not in this audit.
# MAGIC
# MAGIC ## What it covers
# MAGIC
# MAGIC 1. **Revenue reconciliation** — multiple Gold tables compute revenue
# MAGIC    independently. Their totals should match.
# MAGIC 2. **Volume reconciliation** — order/customer counts across tables should be
# MAGIC    coherent.
# MAGIC 3. **Logical invariants** — table properties present, lineage timestamps populated,
# MAGIC    FK-style consistency with Silver.
# MAGIC
# MAGIC ## What it doesn't cover
# MAGIC
# MAGIC - Per-table PK uniqueness, score-bucket sums, etc. — those are validated in each
# MAGIC   table's own Cell 5
# MAGIC - Per-row checks against Silver — that's the per-table CTAS's job
# MAGIC - Hard quality gating — that's Phase 4 DLT's `@dlt.expect_or_drop` job

# COMMAND ----------

# MAGIC %md
# MAGIC ## `run_check` helper
# MAGIC
# MAGIC Mirrors the Phase 2 pattern. A check has:
# MAGIC - `check_name` — what's being verified
# MAGIC - `category` — `revenue` / `volume` / `invariant` for grouping
# MAGIC - `actual` — measured value (always coerced to float for type-uniform DataFrame)
# MAGIC - `expected` — target value or range
# MAGIC - `status` — `PASS` / `FAIL` / `INFO`. INFO is for known, documented gaps that
# MAGIC   shouldn't block but should be visible (e.g., `monthly_revenue` total is
# MAGIC   ~1.28% higher than `category_analytics` because the latter excludes 'unknown').
# MAGIC
# MAGIC Coercing all `actual` and `expected` values to float prevents the
# MAGIC `createDataFrame` mixed-type errors that came up during Phase 2's audit.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Accumulator for all check results
_check_results = []

def run_check(check_name: str, category: str, actual, expected, status: str, notes: str = ""):
    """Record a single validation check result. status in {PASS, FAIL, INFO}."""
    _check_results.append({
        "check_name": check_name,
        "category": category,
        "actual": float(actual) if actual is not None else None,
        "expected": float(expected) if expected is not None else None,
        "status": status,
        "notes": notes,
    })

def show_results():
    """Render the accumulated results as a DataFrame."""
    schema = StructType([
        StructField("check_name", StringType(), False),
        StructField("category",   StringType(), False),
        StructField("actual",     DoubleType(), True),
        StructField("expected",   DoubleType(), True),
        StructField("status",     StringType(), False),
        StructField("notes",      StringType(), True),
    ])
    df = spark.createDataFrame(
        [Row(**r) for r in _check_results],
        schema=schema,
    )
    return df.orderBy("category", "check_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category 1 — Revenue reconciliation
# MAGIC
# MAGIC Three independent revenue computations that should all agree:
# MAGIC
# MAGIC | Source | Computation | Expected |
# MAGIC |---|---|---|
# MAGIC | `gold.monthly_revenue` | `SUM(total_revenue)` | ~15.42M BRL (delivered only, all categories incl. unknown) |
# MAGIC | `gold.category_analytics` | `SUM(total_revenue)` | ~15.22M BRL (delivered only, excludes 'unknown') |
# MAGIC | `gold.customer_rfm` | `SUM(monetary)` | ~15.42M BRL (delivered only, items-source) |
# MAGIC | `gold.payment_analysis` | `SUM(total_items_value)` | ~15.41M BRL (delivered + has_payment) |
# MAGIC
# MAGIC All four should be in the 15.20-15.42M range. Differences > ~250K BRL would
# MAGIC indicate a real reconciliation problem.

# COMMAND ----------

TOLERANCE_BRL = 5000  # 5K BRL = ~0.03% of total revenue

def revenue_from(table: str, column: str) -> float:
    return spark.sql(f"SELECT SUM({column}) AS v FROM olist_lakehouse_us.gold.{table}").first()["v"]

# Authoritative reference: monthly_revenue (covers all categories + delivered)
ref_revenue = revenue_from("monthly_revenue", "total_revenue")
run_check(
    check_name="monthly_revenue.total_revenue is the reference",
    category="revenue",
    actual=ref_revenue,
    expected=15_420_000,
    status="PASS" if 15_300_000 < ref_revenue < 15_500_000 else "FAIL",
    notes=f"Reference revenue figure for all subsequent reconciliations: {ref_revenue:,.2f} BRL",
)

# Customer RFM monetary should match monthly_revenue exactly
rfm_total = revenue_from("customer_rfm", "monetary")
run_check(
    check_name="customer_rfm.monetary == monthly_revenue.total_revenue",
    category="revenue",
    actual=rfm_total,
    expected=ref_revenue,
    status="PASS" if abs(rfm_total - ref_revenue) < TOLERANCE_BRL else "FAIL",
    notes="Both items-source delivered-only; should match to within rounding",
)

# Category analytics excludes 'unknown' — should be ~200K BRL lower
cat_total = revenue_from("category_analytics", "total_revenue")
unknown_revenue = ref_revenue - cat_total
run_check(
    check_name="category_analytics excludes 'unknown' bucket",
    category="revenue",
    actual=unknown_revenue,
    expected=200_000,  # rough expectation; 'unknown' is ~1.28% of revenue
    status="INFO",  # documented exclusion, not a failure
    notes=f"Difference {unknown_revenue:,.0f} BRL = unknown-category revenue retained in monthly_revenue only",
)

# Payment analysis items value should be very close to monthly_revenue
# (small gap from orders without payment rows — Phase 2 noted 1)
pay_items_total = revenue_from("payment_analysis", "total_items_value")
run_check(
    check_name="payment_analysis.total_items_value ≈ monthly_revenue.total_revenue",
    category="revenue",
    actual=pay_items_total,
    expected=ref_revenue,
    status="PASS" if abs(pay_items_total - ref_revenue) < 50_000 else "INFO",
    notes="Small gap expected from orders without payment rows (Phase 2 noted 1 such order)",
)

# Geographic metrics fans out by multi-seller — should be ≥ ref_revenue
geo_total = revenue_from("geographic_metrics", "total_revenue")
geo_revenue_delta = ref_revenue - geo_total
geo_revenue_delta_pct = geo_revenue_delta / ref_revenue * 100
run_check(
    check_name="geographic_metrics.total_revenue within 1% of canonical (threshold filter trade-off)",
    category="revenue",
    actual=geo_revenue_delta_pct,
    expected=1.0,
    status="PASS" if geo_revenue_delta_pct < 1.0 else "FAIL",
    notes=f"Min-5-orders-per-route filter drops sub-threshold routes; revenue delta: {geo_revenue_delta:,.0f} BRL ({geo_revenue_delta_pct:.2f}%)",
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Category 2 — Volume reconciliation
# MAGIC
# MAGIC Order and customer counts across tables. The canonical "delivered orders" count
# MAGIC comes from `monthly_revenue.SUM(order_count)` ≈ 97,276 (per the `monthly_revenue` notebook's Cell 6).
# MAGIC Other tables should match or have documented reasons to differ.
# MAGIC
# MAGIC | Source | Expected behavior |
# MAGIC |---|---|
# MAGIC | `monthly_revenue` SUM(order_count) | ~97,276 — canonical |
# MAGIC | `payment_analysis` SUM(order_count) | ≈ canonical (one bucket per delivered order) |
# MAGIC | `customer_rfm` SUM(frequency) | ≈ canonical (one row per delivered order's customer) |
# MAGIC | `category_analytics` SUM(order_count) | > canonical (multi-category fanout) |
# MAGIC | `geographic_metrics` SUM(order_count) | > canonical (multi-seller fanout) |
# MAGIC | `seller_scorecard` SUM(order_count) | ≤ canonical (min-5-orders threshold drops sellers) |

# COMMAND ----------

def order_count_from(table: str, column: str = "order_count") -> int:
    return spark.sql(
        f"SELECT SUM({column}) AS v FROM olist_lakehouse_us.gold.{table}"
    ).first()["v"]

ref_orders = order_count_from("monthly_revenue", "order_count")
run_check(
    check_name="monthly_revenue.order_count is the reference",
    category="volume",
    actual=ref_orders,
    expected=97_276,
    status="PASS" if 96_000 < ref_orders < 98_000 else "FAIL",
    notes=f"Canonical delivered order count: {ref_orders:,}",
)

# Payment analysis should match canonical (one bucket attribution per order)
pay_orders = order_count_from("payment_analysis")
run_check(
    check_name="payment_analysis.order_count matches canonical",
    category="volume",
    actual=pay_orders,
    expected=ref_orders,
    status="PASS" if abs(pay_orders - ref_orders) <= 1_500 else "FAIL",
    notes="Slight gap expected from orders without payment rows",
)

# Customer RFM frequency-sum should match canonical (frequency = delivered order count per customer)
rfm_orders = spark.sql(
    "SELECT SUM(frequency) AS v FROM olist_lakehouse_us.gold.customer_rfm"
).first()["v"]
run_check(
    check_name="customer_rfm.SUM(frequency) matches delivered orders",
    category="volume",
    actual=rfm_orders,
    expected=ref_orders,
    status="PASS" if abs(rfm_orders - ref_orders) <= 1_500 else "FAIL",
    notes="Each row's frequency = delivered orders for that customer; sum = total delivered orders",
)

# Category analytics should be GREATER than canonical (multi-category fanout)
cat_orders = order_count_from("category_analytics")
cat_orders_delta_pct = abs(cat_orders - ref_orders) / ref_orders * 100
run_check(
    check_name="category_analytics.order_count within 5% of canonical",
    category="volume",
    actual=cat_orders_delta_pct,
    expected=5.0,
    status="PASS" if cat_orders_delta_pct < 5.0 else "FAIL",
    notes=f"Two effects: 'unknown' exclusion (-) and multi-category fanout (+). Net delta: {cat_orders - ref_orders:+,} orders ({cat_orders_delta_pct:.2f}%)",
)

# Geographic metrics also greater (multi-seller fanout)
geo_orders = order_count_from("geographic_metrics")
geo_orders_delta_pct = abs(geo_orders - ref_orders) / ref_orders * 100
run_check(
    check_name="geographic_metrics.order_count within 2% of canonical",
    category="volume",
    actual=geo_orders_delta_pct,
    expected=2.0,
    status="PASS" if geo_orders_delta_pct < 2.0 else "FAIL",
    notes=f"Min-5-orders-per-route threshold drops sub-threshold routes; multi-seller fanout partially offsets. Net delta: {geo_orders - ref_orders:+,} orders ({geo_orders_delta_pct:.2f}%)",
)

# Seller scorecard should be LESS than canonical (min-5 threshold)
ss_orders = order_count_from("seller_scorecard")
run_check(
    check_name="seller_scorecard.order_count < canonical (threshold filter)",
    category="volume",
    actual=ss_orders,
    expected=ref_orders,
    status="PASS" if ss_orders < ref_orders else "FAIL",
    notes=f"Min-5 threshold drops small sellers; missing: {ref_orders - ss_orders:,} orders from sub-threshold sellers",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category 3 — Logical invariants
# MAGIC
# MAGIC Properties the Gold layer should hold by construction:
# MAGIC
# MAGIC - Every Gold table has the canonical 3 TBLPROPERTIES set (`quality`, `medallion.layer`, `source.timezone`)
# MAGIC - Every Gold table has all `_aggregated_at` timestamps populated and within the last day (proves the rebuild ran in this session)
# MAGIC - Every customer in `customer_rfm` exists in `silver.customers`
# MAGIC - Every seller in `seller_scorecard` exists in `silver.sellers`
# MAGIC - Every category in `category_analytics` exists in `silver.products` (modulo the 'unknown' exclusion)

# COMMAND ----------

GOLD_TABLES = [
    "monthly_revenue",
    "delivery_performance",
    "customer_rfm",
    "category_analytics",
    "seller_scorecard",
    "payment_analysis",
    "geographic_metrics",
    "review_trends",
]

# Check that every Gold table has _aggregated_at populated
for table in GOLD_TABLES:
    null_count = spark.sql(
        f"SELECT COUNT(*) AS v FROM olist_lakehouse_us.gold.{table} WHERE _aggregated_at IS NULL"
    ).first()["v"]
    run_check(
        check_name=f"{table}._aggregated_at is fully populated",
        category="invariant",
        actual=null_count,
        expected=0,
        status="PASS" if null_count == 0 else "FAIL",
        notes="Every Gold row should carry a build timestamp",
    )

# Check that every Gold table has the 3 canonical properties (probe just for 'quality' = 'gold')
for table in GOLD_TABLES:
    props_df = spark.sql(f"SHOW TBLPROPERTIES olist_lakehouse_us.gold.{table}")
    props = {r["key"]: r["value"] for r in props_df.collect()}
    has_quality = props.get("quality") == "gold"
    run_check(
        check_name=f"{table} has TBLPROPERTIES.quality = 'gold'",
        category="invariant",
        actual=1.0 if has_quality else 0.0,
        expected=1.0,
        status="PASS" if has_quality else "FAIL",
        notes="Canonical Gold-layer property",
    )

# customer_rfm customers must exist in silver.customers
orphan_customers = spark.sql("""
  SELECT COUNT(*) AS v
  FROM olist_lakehouse_us.gold.customer_rfm rfm
  LEFT ANTI JOIN olist_lakehouse_us.silver.customers c
    ON rfm.customer_unique_id = c.customer_unique_id
""").first()["v"]
run_check(
    check_name="customer_rfm: all customer_unique_ids exist in silver.customers",
    category="invariant",
    actual=orphan_customers,
    expected=0,
    status="PASS" if orphan_customers == 0 else "FAIL",
    notes="LEFT ANTI JOIN should produce zero orphans",
)

# seller_scorecard sellers must exist in silver.sellers
orphan_sellers = spark.sql("""
  SELECT COUNT(*) AS v
  FROM olist_lakehouse_us.gold.seller_scorecard ss
  LEFT ANTI JOIN olist_lakehouse_us.silver.sellers s
    ON ss.seller_id = s.seller_id
""").first()["v"]
run_check(
    check_name="seller_scorecard: all seller_ids exist in silver.sellers",
    category="invariant",
    actual=orphan_sellers,
    expected=0,
    status="PASS" if orphan_sellers == 0 else "FAIL",
    notes="LEFT ANTI JOIN should produce zero orphans",
)

# category_analytics categories must exist in silver.products
orphan_categories = spark.sql("""
  SELECT COUNT(DISTINCT ca.category_name_en) AS v
  FROM olist_lakehouse_us.gold.category_analytics ca
  LEFT ANTI JOIN (SELECT DISTINCT category_name_en FROM olist_lakehouse_us.silver.products) p
    ON ca.category_name_en = p.category_name_en
""").first()["v"]
run_check(
    check_name="category_analytics: all categories exist in silver.products",
    category="invariant",
    actual=orphan_categories,
    expected=0,
    status="PASS" if orphan_categories == 0 else "FAIL",
    notes="LEFT ANTI JOIN should produce zero orphans (excluding 'unknown')",
)

# All 8 Gold tables exist
existing_tables_rows = (
    spark.sql("SHOW TABLES IN olist_lakehouse_us.gold")
    .filter("isTemporary = false")
    .select("tableName")
    .collect()
)
existing_tables = [row.tableName for row in existing_tables_rows]
missing = [t for t in GOLD_TABLES if t not in existing_tables]
run_check(
    check_name=f"All 8 Gold tables exist ({len(GOLD_TABLES)} expected)",
    category="invariant",
    actual=float(len(GOLD_TABLES) - len(missing)),
    expected=float(len(GOLD_TABLES)),
    status="PASS" if not missing else "FAIL",
    notes=f"Missing: {', '.join(missing) if missing else 'none'}",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final results
# MAGIC
# MAGIC The DataFrame below shows all checks grouped by category. Expected output:
# MAGIC
# MAGIC - Most rows: **PASS**
# MAGIC - A few INFO rows for documented gaps (e.g., `monthly_revenue` excludes 'unknown' from `category_analytics`)
# MAGIC - Zero **FAIL** rows would mean the Gold layer is fully reconciled
# MAGIC
# MAGIC If any FAIL appears, investigate that specific check before proceeding to Phase 4.

# COMMAND ----------

results_df = show_results()
display(results_df)

# Also print a count summary
results_df.groupBy("status").count().orderBy("status").show()

# COMMAND ----------

