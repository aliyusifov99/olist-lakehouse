# Databricks notebook source
# MAGIC %md
# MAGIC # SDP Gold Layer — Business-Ready Materialized Views
# MAGIC
# MAGIC Recreates Phase 3's 8 Gold tables as SDP materialized views. Every table is
# MAGIC a `@dp.materialized_view` (Gold is aggregation by definition), every read
# MAGIC from Silver is `spark.read` (batch — required by materialized views).
# MAGIC
# MAGIC ## What this notebook demonstrates
# MAGIC
# MAGIC | Pattern | Where |
# MAGIC |---|---|
# MAGIC | `@dp.materialized_view` for aggregations | All 8 tables |
# MAGIC | `@dp.temporary_view` for shared intermediate result | `customer_rfm` |
# MAGIC | DataFrame API approach | `customer_rfm` |
# MAGIC | `spark.sql(...)` approach | The other 7 |
# MAGIC | Auto DAG inference from SQL string | All 8 |
# MAGIC | Expectations as logic invariants (not data quality) | Multiple tables |
# MAGIC
# MAGIC ## References
# MAGIC - [Materialized views](https://docs.databricks.com/aws/en/ldp/materialized-views)
# MAGIC - [Develop pipeline code with Python](https://docs.databricks.com/aws/en/ldp/python-ref)
# MAGIC - [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/ldp/expectations)

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, expr, current_timestamp, datediff, max as spark_max, min as spark_min,
    sum as spark_sum, count, countDistinct, avg, round as spark_round,
    when, ntile, lit, concat,
)
from pyspark.sql.window import Window

# COMMAND ----------

GOLD_PROPS = {
    "quality": "gold",
    "medallion.layer": "gold",
    "source.timezone": "America/Sao_Paulo",
}


# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold_monthly_revenue_dlt` — month × category revenue
# MAGIC
# MAGIC Mirrors Phase 3's `gold.monthly_revenue`. Joins three Silver tables;
# MAGIC `delivered`-only filter; `SUM(price + freight_value)` as canonical revenue.
# MAGIC
# MAGIC ### Why `spark.sql(...)` here
# MAGIC
# MAGIC The query is 95% identical to Phase 3's. Re-writing in DataFrame API would
# MAGIC mean translating `DATE_TRUNC('MONTH', ...)`, `COUNT(DISTINCT ...)`, `ROUND`,
# MAGIC and a 5-column `GROUP BY` into chained calls. Net effect: same execution
# MAGIC plan, more typing, harder to compare to Phase 3 in code review.
# MAGIC
# MAGIC ### DAG inference works on SQL strings
# MAGIC
# MAGIC SDP's planner reads the SQL inside `spark.sql(...)`, finds the table
# MAGIC references (`silver_orders_dlt`, `silver_order_items_dlt`,
# MAGIC `silver_products_dlt`), and adds them to the dependency graph automatically.
# MAGIC No manual wiring needed.

# COMMAND ----------

@dp.materialized_view(
    name="gold_monthly_revenue_dlt",
    comment="Monthly revenue by product category. PK: (month_start, category_name_en).",
    table_properties=GOLD_PROPS,
)
@dp.expect("non_negative_revenue", "total_revenue >= 0")
@dp.expect("non_negative_orders", "order_count >= 0")
def gold_monthly_revenue_dlt():
    return spark.sql("""
        SELECT
            o.order_year,
            o.order_month,
            o.order_quarter,
            DATE_TRUNC('MONTH', o.order_date) AS month_start,
            p.category_name_en,
            COUNT(DISTINCT o.order_id) AS order_count,
            COUNT(DISTINCT o.customer_id) AS customer_count,
            ROUND(SUM(oi.price), 2) AS total_product_revenue,
            ROUND(SUM(oi.freight_value), 2) AS total_freight_revenue,
            ROUND(SUM(oi.price + oi.freight_value), 2) AS total_revenue,
            ROUND(AVG(oi.price), 2) AS avg_order_item_value,
            CURRENT_TIMESTAMP() AS _aggregated_at
        FROM silver_orders_dlt o
        INNER JOIN silver_order_items_dlt oi ON o.order_id = oi.order_id
        INNER JOIN silver_products_dlt p ON oi.product_id = p.product_id
        WHERE o.order_status = 'delivered'
        GROUP BY
            o.order_year, o.order_month, o.order_quarter,
            DATE_TRUNC('MONTH', o.order_date),
            p.category_name_en
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold_delivery_performance_dlt` — delivery metrics by seller state
# MAGIC
# MAGIC Mirrors Phase 3's `gold.delivery_performance`. Min-volume threshold (≥10
# MAGIC orders per state) preserved. Reuses `silver.delivery_sla_status` UDF for
# MAGIC the SLA bucket distribution.

# COMMAND ----------

@dp.materialized_view(
    name="gold_delivery_performance_dlt",
    comment="Delivery performance by seller state. Min 10 orders per state. Uses delivery_sla_status UDF.",
    table_properties=GOLD_PROPS,
)
@dp.expect("non_negative_late_pct", "late_delivery_pct >= 0")
@dp.expect("late_pct_under_100", "late_delivery_pct <= 100")
def gold_delivery_performance_dlt():
    return spark.sql("""
        SELECT
            s.seller_state,
            COUNT(DISTINCT o.order_id) AS total_orders,
            ROUND(AVG(o.delivery_days), 2) AS avg_delivery_days,
            ROUND(AVG(o.delivery_delay_days), 2) AS avg_delay_days,
            ROUND(
                100.0 * SUM(CASE WHEN o.is_late_delivery = TRUE THEN 1 ELSE 0 END) / COUNT(*),
                2
            ) AS late_delivery_pct,
            ROUND(
                100.0 * SUM(CASE WHEN o.sla_status = 'early' THEN 1 ELSE 0 END) / COUNT(*),
                2
            ) AS early_pct,
            ROUND(
                100.0 * SUM(CASE WHEN o.sla_status = 'on_time' THEN 1 ELSE 0 END) / COUNT(*),
                2
            ) AS on_time_pct,
            ROUND(
                100.0 * SUM(CASE WHEN o.sla_status = 'slightly_late' THEN 1 ELSE 0 END) / COUNT(*),
                2
            ) AS slightly_late_pct,
            ROUND(
                100.0 * SUM(CASE WHEN o.sla_status = 'very_late' THEN 1 ELSE 0 END) / COUNT(*),
                2
            ) AS very_late_pct,
            ROUND(AVG(oi.freight_value), 2) AS avg_freight_cost,
            CURRENT_TIMESTAMP() AS _aggregated_at
        FROM silver_orders_dlt o
        INNER JOIN silver_order_items_dlt oi ON o.order_id = oi.order_id
        INNER JOIN silver_sellers_dlt s ON oi.seller_id = s.seller_id
        WHERE o.order_status = 'delivered'
        GROUP BY s.seller_state
        HAVING COUNT(DISTINCT o.order_id) >= 10
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold_customer_rfm_dlt` — RFM segmentation
# MAGIC
# MAGIC ### Why this one uses `@dp.temporary_view` + DataFrame API
# MAGIC
# MAGIC Two reasons. First, `customer_rfm` has a natural two-step shape: compute
# MAGIC per-customer metrics, then assign NTILE quintiles + segment labels. That's
# MAGIC a clean fit for a `customer_metrics_temp_dlt` intermediate temporary view
# MAGIC that the final view consumes.
# MAGIC
# MAGIC Second, this notebook so far has been all `spark.sql(...)`. One DataFrame-API
# MAGIC table demonstrates fluency in both approaches.
# MAGIC
# MAGIC ### About `@dp.temporary_view`
# MAGIC
# MAGIC Temporary views in SDP are NOT published to Unity Catalog. They exist only
# MAGIC during the pipeline run. They appear in the DAG (helpful for debugging) but
# MAGIC don't show up in Catalog Explorer. Use them when you have an intermediate
# MAGIC result that's referenced by exactly one downstream consumer and isn't worth
# MAGIC persisting on its own.
# MAGIC
# MAGIC If the intermediate were used by *multiple* consumers, you'd promote it to
# MAGIC a regular `@dp.materialized_view` so the computation happens once.
# MAGIC
# MAGIC ### F-degeneracy preserved
# MAGIC
# MAGIC Per Phase 3: 97% of customers are F=1, NTILE(5) on F is meaningless within
# MAGIC the F=1 mass. We surface this via `is_repeat_customer` flag rather than
# MAGIC fixing it. Production fix is `DENSE_RANK` on monetary value within the F=1
# MAGIC group; out of scope for the SDP rebuild.

# COMMAND ----------

@dp.temporary_view(
    name="customer_metrics_temp_dlt",
    comment="Intermediate per-customer R/F/M metrics. Consumed only by gold_customer_rfm_dlt.",
)
def customer_metrics_temp_dlt():
    orders = spark.read.table("silver_orders_dlt").filter(col("order_status") == "delivered")
    customers = spark.read.table("silver_customers_dlt")
    payments = spark.read.table("silver_payments_dlt")

    # Compute the reference date as a 1-row DataFrame and cross-join it.
    # This keeps everything inside the Spark plan — no .collect() side-effects.
    max_date_df = orders.agg(spark_max("order_date").alias("max_date"))

    return (
        orders.alias("o")
        .join(customers.alias("c"), col("o.customer_id") == col("c.customer_id"))
        .join(payments.alias("p"), col("o.order_id") == col("p.order_id"))
        .crossJoin(max_date_df)  # adds the literal max_date column to every row
        .groupBy(col("c.customer_unique_id"), col("c.customer_state"), col("c.customer_city"))
        .agg(
            datediff(spark_max(col("max_date")), spark_max(col("o.order_date"))).alias("recency_days"),
            countDistinct(col("o.order_id")).alias("frequency"),
            spark_round(spark_sum(col("p.payment_value")), 2).alias("monetary"),
        )
    )

# COMMAND ----------

@dp.materialized_view(
    name="gold_customer_rfm_dlt",
    comment="RFM segmentation at customer_unique_id grain. F-degeneracy surfaced via is_repeat_customer.",
    table_properties=GOLD_PROPS,
)
@dp.expect_or_fail("valid_customer_unique_id", "customer_unique_id IS NOT NULL")
@dp.expect("recency_non_negative", "recency_days >= 0")
@dp.expect("frequency_positive", "frequency >= 1")
def gold_customer_rfm_dlt():
    metrics = spark.read.table("customer_metrics_temp_dlt")

    # NTILE windows — global ordering across all customers
    w_recency = Window.orderBy(col("recency_days").desc())   # higher score = more recent
    w_frequency = Window.orderBy(col("frequency"))           # higher score = more frequent
    w_monetary = Window.orderBy(col("monetary"))             # higher score = higher spend

    scored = (
        metrics
        .withColumn("r_score", ntile(5).over(w_recency))
        .withColumn("f_score", ntile(5).over(w_frequency))
        .withColumn("m_score", ntile(5).over(w_monetary))
    )

    return (
        scored
        .withColumn(
            "rfm_combined",
            concat(col("r_score"), col("f_score"), col("m_score")),
        )
        .withColumn("is_repeat_customer", col("frequency") > 1)
        .withColumn(
            "customer_segment",
            when((col("r_score") >= 4) & (col("f_score") >= 4) & (col("m_score") >= 4), lit("Champions"))
            .when((col("r_score") >= 3) & (col("f_score") >= 3) & (col("m_score") >= 3), lit("Loyal Customers"))
            .when((col("r_score") >= 4) & (col("f_score") <= 2), lit("New Customers"))
            .when((col("r_score") <= 2) & (col("f_score") >= 3), lit("At Risk"))
            .when((col("r_score") <= 2) & (col("f_score") <= 2) & (col("m_score") <= 2), lit("Lost"))
            .otherwise(lit("Potential Loyalists")),
        )
        .withColumn("_aggregated_at", current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold_category_analytics_dlt` — revenue × review × delivery per category
# MAGIC
# MAGIC Phase 3's three-way correlation table. Excludes 'unknown' category per
# MAGIC Phase 3 convention (those rows are surfaced separately, not analyzed).
# MAGIC Uses Phase 2's `classify_review_sentiment` UDF.

# COMMAND ----------

@dp.materialized_view(
    name="gold_category_analytics_dlt",
    comment="Per-category revenue, avg review, avg delivery, sentiment distribution. Excludes 'unknown'.",
    table_properties=GOLD_PROPS,
)
@dp.expect("non_negative_revenue", "total_revenue >= 0")
@dp.expect("avg_score_in_range", "avg_review_score IS NULL OR avg_review_score BETWEEN 1 AND 5")
def gold_category_analytics_dlt():
    return spark.sql("""
        WITH order_category AS (
            SELECT DISTINCT
                oi.order_id,
                p.category_name_en
            FROM silver_order_items_dlt oi
            INNER JOIN silver_products_dlt p ON oi.product_id = p.product_id
            WHERE p.category_name_en != 'unknown'
        ),
        category_revenue AS (
            SELECT
                p.category_name_en,
                COUNT(DISTINCT o.order_id) AS order_count,
                ROUND(SUM(oi.price + oi.freight_value), 2) AS total_revenue,
                ROUND(AVG(oi.price), 2) AS avg_item_price
            FROM silver_orders_dlt o
            INNER JOIN silver_order_items_dlt oi ON o.order_id = oi.order_id
            INNER JOIN silver_products_dlt p ON oi.product_id = p.product_id
            WHERE o.order_status = 'delivered' AND p.category_name_en != 'unknown'
            GROUP BY p.category_name_en
        ),
        category_reviews AS (
            SELECT
                oc.category_name_en,
                ROUND(AVG(r.review_score), 2) AS avg_review_score,
                COUNT(r.review_id) AS review_count,
                ROUND(
                    100.0 * SUM(CASE WHEN r.sentiment IN ('positive', 'promoter') THEN 1 ELSE 0 END) / COUNT(*),
                    2
                ) AS positive_sentiment_pct,
                ROUND(
                    100.0 * SUM(CASE WHEN r.sentiment = 'negative' THEN 1 ELSE 0 END) / COUNT(*),
                    2
                ) AS negative_sentiment_pct
            FROM order_category oc
            INNER JOIN silver_reviews_dlt r ON oc.order_id = r.order_id
            GROUP BY oc.category_name_en
        ),
        category_delivery AS (
            SELECT
                oc.category_name_en,
                ROUND(AVG(o.delivery_days), 2) AS avg_delivery_days,
                ROUND(
                    100.0 * SUM(CASE WHEN o.is_late_delivery = TRUE THEN 1 ELSE 0 END) / COUNT(*),
                    2
                ) AS late_delivery_pct
            FROM order_category oc
            INNER JOIN silver_orders_dlt o ON oc.order_id = o.order_id
            WHERE o.order_status = 'delivered'
            GROUP BY oc.category_name_en
        )
        SELECT
            cr.category_name_en,
            cr.order_count,
            cr.total_revenue,
            cr.avg_item_price,
            crv.avg_review_score,
            crv.review_count,
            crv.positive_sentiment_pct,
            crv.negative_sentiment_pct,
            cd.avg_delivery_days,
            cd.late_delivery_pct,
            CURRENT_TIMESTAMP() AS _aggregated_at
        FROM category_revenue cr
        LEFT JOIN category_reviews crv ON cr.category_name_en = crv.category_name_en
        LEFT JOIN category_delivery cd ON cr.category_name_en = cd.category_name_en
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold_seller_scorecard_dlt` — composite seller score
# MAGIC
# MAGIC Phase 3's three-component composite (40% reviews, 30% delivery speed,
# MAGIC 30% volume). Min 5 orders per seller. Tier classification via NTILE(5)
# MAGIC on composite_score.

# COMMAND ----------

@dp.materialized_view(
    name="gold_seller_scorecard_dlt",
    comment="Per-seller composite score: 40% reviews + 30% delivery speed + 30% volume. Min 5 orders.",
    table_properties=GOLD_PROPS,
)
@dp.expect_or_fail("composite_score_in_range", "composite_score BETWEEN 0 AND 100")
@dp.expect("avg_score_in_range", "avg_review_score IS NULL OR avg_review_score BETWEEN 1 AND 5")
def gold_seller_scorecard_dlt():
    return spark.sql("""
        WITH order_seller AS (
            SELECT DISTINCT
                oi.order_id,
                oi.seller_id
            FROM silver_order_items_dlt oi
        ),
        seller_metrics AS (
            SELECT
                s.seller_id,
                s.seller_city,
                s.seller_state,
                COUNT(DISTINCT oi.order_id) AS total_orders,
                COUNT(DISTINCT oi.product_id) AS unique_products_sold,
                ROUND(SUM(oi.price), 2) AS total_revenue,
                ROUND(AVG(o.delivery_days), 2) AS avg_delivery_days,
                ROUND(
                    100.0 * SUM(CASE WHEN o.is_late_delivery = TRUE THEN 1 ELSE 0 END) / COUNT(*),
                    2
                ) AS late_delivery_pct
            FROM silver_sellers_dlt s
            INNER JOIN silver_order_items_dlt oi ON s.seller_id = oi.seller_id
            INNER JOIN silver_orders_dlt o ON oi.order_id = o.order_id
            WHERE o.order_status = 'delivered'
            GROUP BY s.seller_id, s.seller_city, s.seller_state
            HAVING COUNT(DISTINCT oi.order_id) >= 5
        ),
        seller_reviews AS (
            SELECT
                os.seller_id,
                ROUND(AVG(r.review_score), 2) AS avg_review_score
            FROM order_seller os
            INNER JOIN silver_reviews_dlt r ON os.order_id = r.order_id
            GROUP BY os.seller_id
        ),
        seller_combined AS (
            SELECT
                sm.*,
                sr.avg_review_score,
                -- Components (kept as separate columns for transparency)
                ROUND(COALESCE(sr.avg_review_score, 3) / 5 * 40, 2) AS review_component,
                ROUND(
                    CASE
                        WHEN sm.avg_delivery_days <= 7 THEN 30
                        WHEN sm.avg_delivery_days <= 14 THEN 20
                        WHEN sm.avg_delivery_days <= 21 THEN 10
                        ELSE 0
                    END, 2
                ) AS delivery_component,
                ROUND(LEAST(sm.total_orders / 100.0, 1.0) * 30, 2) AS volume_component
            FROM seller_metrics sm
            LEFT JOIN seller_reviews sr ON sm.seller_id = sr.seller_id
        )
        SELECT
            *,
            ROUND(review_component + delivery_component + volume_component, 2) AS composite_score,
            CASE NTILE(5) OVER (ORDER BY (review_component + delivery_component + volume_component))
                WHEN 5 THEN 'top_20pct'
                WHEN 4 THEN 'second_20pct'
                WHEN 3 THEN 'middle_20pct'
                WHEN 2 THEN 'fourth_20pct'
                WHEN 1 THEN 'bottom_20pct'
            END AS performance_tier,
            CURRENT_TIMESTAMP() AS _aggregated_at
        FROM seller_combined
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold_payment_analysis_dlt` — payment method × installment bucket
# MAGIC
# MAGIC Phase 3 found this is sparse — only credit_card supports installments,
# MAGIC so 8 rows total (not the ~25 you'd expect from 5 types × 5 buckets).
# MAGIC `delivered`-only filter; first-payment attribution.

# COMMAND ----------

@dp.materialized_view(
    name="gold_payment_analysis_dlt",
    comment="Payment type × installment bucket distribution. Sparse — only credit_card supports installments.",
    table_properties=GOLD_PROPS,
)
@dp.expect("non_negative_count", "order_count >= 0")
def gold_payment_analysis_dlt():
    return spark.sql("""
        WITH first_payment AS (
            SELECT
                p.order_id,
                p.payment_type,
                p.installment_bucket,
                p.payment_value,
                p.payment_type_known
            FROM silver_payments_dlt p
            WHERE p.payment_sequential = 1
        ),
        order_items_total AS (
            SELECT
                order_id,
                SUM(total_item_value) AS items_total
            FROM silver_order_items_dlt
            GROUP BY order_id
        )
        SELECT
            fp.payment_type,
            fp.installment_bucket,
            COUNT(*) AS order_count,
            ROUND(SUM(fp.payment_value), 2) AS total_paid,
            ROUND(AVG(fp.payment_value), 2) AS avg_order_value,
            ROUND(SUM(oit.items_total), 2) AS total_items,
            ROUND(SUM(fp.payment_value - oit.items_total), 2) AS items_payment_gap,
            CURRENT_TIMESTAMP() AS _aggregated_at
        FROM first_payment fp
        INNER JOIN silver_orders_dlt o ON fp.order_id = o.order_id
        LEFT JOIN order_items_total oit ON fp.order_id = oit.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY fp.payment_type, fp.installment_bucket
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold_geographic_metrics_dlt` — customer-state × seller-state route
# MAGIC
# MAGIC Phase 3's hub-and-spoke insight table. Min 5 orders per route. Note:
# MAGIC LEFT JOIN to geolocation handles the 158 customer + 7 seller zip prefixes
# MAGIC with no centroid, but the route-level grain isn't sensitive to zip
# MAGIC coverage gaps — state-level aggregation absorbs them.

# COMMAND ----------

@dp.materialized_view(
    name="gold_geographic_metrics_dlt",
    comment="Customer state × seller state freight, delivery, and revenue. Min 5 orders per route.",
    table_properties=GOLD_PROPS,
)
@dp.expect("non_negative_orders", "order_count >= 0")
@dp.expect("non_negative_freight", "avg_freight_cost IS NULL OR avg_freight_cost >= 0")
def gold_geographic_metrics_dlt():
    return spark.sql("""
        SELECT
            c.customer_state,
            s.seller_state,
            CASE WHEN c.customer_state = s.seller_state THEN 'intra_state' ELSE 'cross_state' END AS route_type,
            COUNT(DISTINCT o.order_id) AS order_count,
            ROUND(SUM(oi.price + oi.freight_value), 2) AS total_revenue,
            ROUND(AVG(oi.freight_value), 2) AS avg_freight_cost,
            ROUND(AVG(o.delivery_days), 2) AS avg_delivery_days,
            ROUND(
                100.0 * SUM(CASE WHEN o.is_late_delivery = TRUE THEN 1 ELSE 0 END) / COUNT(*),
                2
            ) AS late_delivery_pct,
            CURRENT_TIMESTAMP() AS _aggregated_at
        FROM silver_orders_dlt o
        INNER JOIN silver_customers_dlt c ON o.customer_id = c.customer_id
        INNER JOIN silver_order_items_dlt oi ON o.order_id = oi.order_id
        INNER JOIN silver_sellers_dlt s ON oi.seller_id = s.seller_id
        WHERE o.order_status = 'delivered'
        GROUP BY c.customer_state, s.seller_state
        HAVING COUNT(DISTINCT o.order_id) >= 5
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold_review_trends_dlt` — review trends by month
# MAGIC
# MAGIC Phase 3's review-trends table. Includes the score-distribution columns
# MAGIC (score_1_pct through score_5_pct) plus avg score and sentiment breakdown.
# MAGIC The score percentages should sum to 100 ± rounding — Phase 3 verified this.

# COMMAND ----------

@dp.materialized_view(
    name="gold_review_trends_dlt",
    comment="Review trends by month. Score distribution percentages sum to ~100 (within rounding).",
    table_properties=GOLD_PROPS,
)
@dp.expect("avg_score_in_range", "avg_review_score BETWEEN 1 AND 5")
@dp.expect("score_distribution_sums_to_100",
    "ABS(score_1_pct + score_2_pct + score_3_pct + score_4_pct + score_5_pct - 100) < 0.5")
def gold_review_trends_dlt():
    return spark.sql("""
        SELECT
            DATE_TRUNC('MONTH', r.review_creation_ts) AS review_month_start,
            COUNT(*) AS review_count,
            ROUND(AVG(r.review_score), 2) AS avg_review_score,
            ROUND(100.0 * SUM(CASE WHEN r.review_score = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS score_1_pct,
            ROUND(100.0 * SUM(CASE WHEN r.review_score = 2 THEN 1 ELSE 0 END) / COUNT(*), 2) AS score_2_pct,
            ROUND(100.0 * SUM(CASE WHEN r.review_score = 3 THEN 1 ELSE 0 END) / COUNT(*), 2) AS score_3_pct,
            ROUND(100.0 * SUM(CASE WHEN r.review_score = 4 THEN 1 ELSE 0 END) / COUNT(*), 2) AS score_4_pct,
            ROUND(100.0 * SUM(CASE WHEN r.review_score = 5 THEN 1 ELSE 0 END) / COUNT(*), 2) AS score_5_pct,
            ROUND(100.0 * SUM(CASE WHEN r.sentiment = 'negative' THEN 1 ELSE 0 END) / COUNT(*), 2) AS negative_pct,
            ROUND(100.0 * SUM(CASE WHEN r.sentiment IN ('positive', 'promoter') THEN 1 ELSE 0 END) / COUNT(*), 2) AS positive_pct,
            CURRENT_TIMESTAMP() AS _aggregated_at
        FROM silver_reviews_dlt r
        WHERE r.review_creation_ts IS NOT NULL
        GROUP BY DATE_TRUNC('MONTH', r.review_creation_ts)
    """)
