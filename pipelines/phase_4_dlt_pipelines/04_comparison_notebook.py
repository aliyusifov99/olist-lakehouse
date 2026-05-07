# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 — Comparison: Imperative (Phase 1–3) vs Declarative (Phase 4 SDP)
# MAGIC
# MAGIC This notebook validates that the SDP rebuild produces equivalent business outputs
# MAGIC to the manually-orchestrated Phase 1–3 pipeline. Both pipelines consumed the same
# MAGIC GCS landing data; both produced 25 tables. This notebook compares them at three
# MAGIC levels:
# MAGIC
# MAGIC 1. **Layer-level row counts** — does each layer produce the same number of rows?
# MAGIC 2. **Per-table row counts** — table-by-table breakdown, with computed deltas
# MAGIC 3. **Per-table content** — for key Gold tables, are the actual aggregate values equal?
# MAGIC
# MAGIC ## Expected differences (documented up front)
# MAGIC
# MAGIC | Table | Expected delta | Cause |
# MAGIC |---|---|---|
# MAGIC | `gold_customer_rfm_dlt` | +112 rows vs `gold.customer_rfm` | SDP version filters orders to `delivered` before RFM aggregation; Phase 3 used a slightly different scope. Documented in Phase 4 notes. |
# MAGIC
# MAGIC Everything else should match exactly.

# COMMAND ----------

from pyspark.sql.functions import col

# Define the table mapping. Left = Phase 1-3 imperative, right = Phase 4 SDP.
COMPARISON_TABLES = {
    "bronze": [
        ("olist_lakehouse_us.bronze.orders",                "olist_lakehouse_us.dlt_output.bronze_orders_dlt"),
        ("olist_lakehouse_us.bronze.order_items",           "olist_lakehouse_us.dlt_output.bronze_order_items_dlt"),
        ("olist_lakehouse_us.bronze.payments",              "olist_lakehouse_us.dlt_output.bronze_payments_dlt"),
        ("olist_lakehouse_us.bronze.reviews",               "olist_lakehouse_us.dlt_output.bronze_reviews_dlt"),
        ("olist_lakehouse_us.bronze.products",              "olist_lakehouse_us.dlt_output.bronze_products_dlt"),
        ("olist_lakehouse_us.bronze.customers",             "olist_lakehouse_us.dlt_output.bronze_customers_dlt"),
        ("olist_lakehouse_us.bronze.sellers",               "olist_lakehouse_us.dlt_output.bronze_sellers_dlt"),
        ("olist_lakehouse_us.bronze.geolocation",           "olist_lakehouse_us.dlt_output.bronze_geolocation_dlt"),
        ("olist_lakehouse_us.bronze.category_translation",  "olist_lakehouse_us.dlt_output.bronze_category_translation_dlt"),
    ],
    "silver": [
        ("olist_lakehouse_us.silver.orders",                "olist_lakehouse_us.dlt_output.silver_orders_dlt"),
        ("olist_lakehouse_us.silver.order_items",           "olist_lakehouse_us.dlt_output.silver_order_items_dlt"),
        ("olist_lakehouse_us.silver.payments",              "olist_lakehouse_us.dlt_output.silver_payments_dlt"),
        ("olist_lakehouse_us.silver.reviews",               "olist_lakehouse_us.dlt_output.silver_reviews_dlt"),
        ("olist_lakehouse_us.silver.products",              "olist_lakehouse_us.dlt_output.silver_products_dlt"),
        ("olist_lakehouse_us.silver.customers",             "olist_lakehouse_us.dlt_output.silver_customers_dlt"),
        ("olist_lakehouse_us.silver.sellers",               "olist_lakehouse_us.dlt_output.silver_sellers_dlt"),
        ("olist_lakehouse_us.silver.geolocation",           "olist_lakehouse_us.dlt_output.silver_geolocation_dlt"),
    ],
    "gold": [
        ("olist_lakehouse_us.gold.monthly_revenue",         "olist_lakehouse_us.dlt_output.gold_monthly_revenue_dlt"),
        ("olist_lakehouse_us.gold.delivery_performance",    "olist_lakehouse_us.dlt_output.gold_delivery_performance_dlt"),
        ("olist_lakehouse_us.gold.customer_rfm",            "olist_lakehouse_us.dlt_output.gold_customer_rfm_dlt"),
        ("olist_lakehouse_us.gold.category_analytics",      "olist_lakehouse_us.dlt_output.gold_category_analytics_dlt"),
        ("olist_lakehouse_us.gold.seller_scorecard",        "olist_lakehouse_us.dlt_output.gold_seller_scorecard_dlt"),
        ("olist_lakehouse_us.gold.payment_analysis",        "olist_lakehouse_us.dlt_output.gold_payment_analysis_dlt"),
        ("olist_lakehouse_us.gold.geographic_metrics",      "olist_lakehouse_us.dlt_output.gold_geographic_metrics_dlt"),
        ("olist_lakehouse_us.gold.review_trends",           "olist_lakehouse_us.dlt_output.gold_review_trends_dlt"),
    ],
}

def count_table(name):
    """Return the row count of a table, or None if it doesn't exist."""
    try:
        return spark.table(name).count()
    except Exception:
        return None

# Compute per-layer aggregate row counts
print(f"{'Layer':<10} {'Phase 1-3':>15} {'Phase 4 (SDP)':>15} {'Δ':>10} {'%':>8}")
print("-" * 60)
for layer, pairs in COMPARISON_TABLES.items():
    p3_total = sum(count_table(p3) or 0 for p3, _ in pairs)
    p4_total = sum(count_table(p4) or 0 for _, p4 in pairs)
    delta = p4_total - p3_total
    pct = (delta / p3_total * 100) if p3_total > 0 else 0
    print(f"{layer:<10} {p3_total:>15,} {p4_total:>15,} {delta:>+10,} {pct:>+7.3f}%")

# COMMAND ----------

from pyspark.sql import Row

results = []
for layer, pairs in COMPARISON_TABLES.items():
    for p3_name, p4_name in pairs:
        p3_count = count_table(p3_name)
        p4_count = count_table(p4_name)
        delta = (p4_count - p3_count) if (p3_count is not None and p4_count is not None) else None
        results.append(Row(
            layer=layer,
            p3_table=p3_name.split(".")[-1],
            p4_table=p4_name.split(".")[-1],
            p3_count=p3_count,
            p4_count=p4_count,
            delta=delta,
            status="MATCH" if delta == 0 else ("DELTA" if delta is not None else "ERROR")
        ))

display(spark.createDataFrame(results))

# COMMAND ----------

# Compare canonical revenue totals
p3_revenue = spark.sql("""
    SELECT ROUND(SUM(total_revenue), 2) AS total
    FROM olist_lakehouse_us.gold.monthly_revenue
""").collect()[0][0]

p4_revenue = spark.sql("""
    SELECT ROUND(SUM(total_revenue), 2) AS total
    FROM olist_lakehouse_us.dlt_output.gold_monthly_revenue_dlt
""").collect()[0][0]

print(f"Total revenue (Phase 1-3):   {p3_revenue:>15,.2f} BRL")
print(f"Total revenue (Phase 4 SDP): {p4_revenue:>15,.2f} BRL")
print(f"Delta:                       {p4_revenue - p3_revenue:>+15,.2f} BRL")
print(f"Match: {abs(p4_revenue - p3_revenue) < 0.01}")

# COMMAND ----------

