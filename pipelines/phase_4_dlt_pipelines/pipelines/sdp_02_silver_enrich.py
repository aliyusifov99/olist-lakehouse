# Databricks notebook source
# MAGIC %md
# MAGIC # SDP Silver Layer — Enrichment Tables
# MAGIC
# MAGIC The three Silver tables that didn't fit the simple "stream Bronze → row-level
# MAGIC transform" pattern of `sdp_02_silver_core`:
# MAGIC
# MAGIC | Table | Mode | Why |
# MAGIC |---|---|---|
# MAGIC | `silver_payments_dlt` | Streaming | Row-level transform with branching logic; preserves Phase 2's anomaly-flagging strategy |
# MAGIC | `silver_reviews_dlt` | Streaming | Row-level transform + UC SQL UDF call; composite PK preserved per Phase 2 |
# MAGIC | `silver_geolocation_dlt` | **Materialized view** | Real aggregation (1M points → 19K centroids) |
# MAGIC
# MAGIC ## References
# MAGIC - [Materialized views in SDP](https://docs.databricks.com/aws/en/ldp/materialized-views)
# MAGIC - [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/ldp/expectations)
# MAGIC - [SQL UDFs in Unity Catalog](https://docs.databricks.com/aws/en/udf/unity-catalog)

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, expr, when, length, trim, lit, current_timestamp,
    avg, round as spark_round, count, min as spark_min, mode,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared TBLPROPERTIES
# MAGIC
# MAGIC Same constants as `sdp_02_silver_core`. Repeated here because each notebook
# MAGIC is its own Python module — there's no "shared imports" file in this layout.
# MAGIC The DRY violation is acceptable; we don't want a `pipelines/_common.py` that
# MAGIC SDP would also try to interpret as a pipeline source file.

# COMMAND ----------

SILVER_PROPS = {
    "quality": "silver",
    "medallion.layer": "silver",
}
SILVER_PROPS_TS = {
    **SILVER_PROPS,
    "source.timezone": "America/Sao_Paulo",
    "delta.feature.timestampNtz": "supported",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver_payments_dlt` — payments with installment buckets and quality flags
# MAGIC
# MAGIC Mirrors Phase 2's `silver.payments`. The conceptual model: **anomalies are
# MAGIC made queryable, not filtered out.** Three Phase-1 anomalies are surfaced via
# MAGIC derived columns rather than dropped:
# MAGIC
# MAGIC | Bronze anomaly | Silver column |
# MAGIC |---|---|
# MAGIC | 9 rows with `payment_value <= 0` | `valid_payment_value` boolean (false for these) |
# MAGIC | 3 rows with `payment_type = 'not_defined'` | `payment_type_known` boolean (false for these) |
# MAGIC | Some rows with `payment_installments = 0` | `installments_normalized` (0 → 1) |
# MAGIC
# MAGIC ### Expectation severity choices
# MAGIC
# MAGIC | Check | Severity | Why |
# MAGIC |---|---|---|
# MAGIC | `order_id IS NOT NULL` | `expect_or_fail` | Required FK |
# MAGIC | `payment_sequential IS NOT NULL` | `expect_or_fail` | Required for composite PK |
# MAGIC | `payment_installments >= 0` | `expect` | Phase 2 confirmed all values are 0 or positive; warn-only as a regression detector |
# MAGIC | `payment_value >= 0` | `expect` | The 9 zero-value rows are documented anomalies, not bugs — keep them visible |
# MAGIC
# MAGIC ### Why we keep the zero-value rows
# MAGIC
# MAGIC Phase 2's audit found 9 rows with `payment_value = 0`. These are real orders
# MAGIC where 100%-voucher coverage zeroed the merchant-side payment. Filtering them
# MAGIC out would distort the items-vs-payments reconciliation that Phase 3's
# MAGIC `payment_analysis` table depends on. Surfacing them via `valid_payment_value`
# MAGIC keeps Gold queries honest.

# COMMAND ----------

@dp.table(
    name="silver_payments_dlt",
    comment=(
        "Payments with installment buckets and quality flags. "
        "Composite PK: (order_id, payment_sequential). "
        "Preserves the 9 zero-value and 3 not_defined rows from Phase 1 — see "
        "valid_payment_value and payment_type_known."
    ),
    table_properties=SILVER_PROPS,
)
@dp.expect_all_or_fail({
    "valid_order_id": "order_id IS NOT NULL",
    "valid_payment_sequential": "payment_sequential IS NOT NULL",
})
@dp.expect_all({
    "non_negative_installments": "payment_installments >= 0",
    "non_negative_value": "payment_value >= 0",
})
def silver_payments_dlt():
    return (
        spark.readStream.table("bronze_payments_dlt")
        .select(
            col("order_id"),
            col("payment_sequential").cast("int").alias("payment_sequential"),
            col("payment_type"),
            col("payment_installments").cast("int").alias("payment_installments"),
            col("payment_value").cast("double").alias("payment_value"),
            col("_ingested_at"),
        )
        # Quality flags — surface anomalies as queryable columns
        .withColumn(
            "payment_type_known",
            (col("payment_type").isNotNull()) & (col("payment_type") != lit("not_defined")),
        )
        .withColumn("valid_payment_value", col("payment_value") > 0)
        # Normalize 0-installment rows to 1 (Phase 1's documented convention)
        .withColumn(
            "installments_normalized",
            when(col("payment_installments") == 0, lit(1)).otherwise(col("payment_installments")),
        )
        # Bucketing: Phase 2's chosen bands
        .withColumn(
            "installment_bucket",
            when(col("installments_normalized") == 1, lit("1"))
            .when(col("installments_normalized").between(2, 3), lit("2-3"))
            .when(col("installments_normalized").between(4, 6), lit("4-6"))
            .when(col("installments_normalized").between(7, 12), lit("7-12"))
            .otherwise(lit("13+")),
        )
        .withColumn("_processed_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver_reviews_dlt` — reviews with sentiment classification
# MAGIC
# MAGIC Mirrors Phase 2's `silver.reviews`. Three behaviors worth pointing out:
# MAGIC
# MAGIC ### 1. Composite PK preserved
# MAGIC
# MAGIC Phase 2's decision: the 814 duplicate `review_id`s are kept under composite
# MAGIC PK `(review_id, order_id)` rather than deduped. Since SDP per-row expectations
# MAGIC can't enforce composite uniqueness anyway, we don't try — the structure is
# MAGIC implicit in the data. Next step's audit will verify this matches Phase 2.
# MAGIC
# MAGIC ### 2. UC SQL UDF call inside SDP
# MAGIC
# MAGIC `silver.classify_review_sentiment(review_score, comment_length)` is called
# MAGIC via `expr(...)` so it stays in Photon. Same fully-qualified-name pattern as
# MAGIC the SLA UDF in `silver_orders_dlt`.
# MAGIC
# MAGIC The UDF was created in Phase 2 and exists in `olist_lakehouse_us.silver` —
# MAGIC SDP doesn't need to know about it ahead of time. UDF references are not part
# MAGIC of the auto-inferred DAG (only table references are), so there's no
# MAGIC dependency arrow drawn for them in the pipeline graph.
# MAGIC
# MAGIC ### 3. The `expect` we *can't* express
# MAGIC
# MAGIC We'd love to express "review_id should be unique" — but per-row predicates
# MAGIC can't see other rows. Instead we get a meaningful close substitute: the
# MAGIC `score_in_range` expectation. Expectations are per-row; full-table invariants
# MAGIC are a audit-notebook concern.

# COMMAND ----------

@dp.table(
    name="silver_reviews_dlt",
    comment=(
        "Reviews with sentiment classification via silver.classify_review_sentiment UDF. "
        "Composite PK: (review_id, order_id). 814 duplicate review_ids preserved per Phase 2."
    ),
    table_properties=SILVER_PROPS_TS,
)
@dp.expect_all_or_fail({
    "valid_review_id": "review_id IS NOT NULL",
    "valid_order_id": "order_id IS NOT NULL",
})
@dp.expect("score_in_range", "review_score BETWEEN 1 AND 5")
def silver_reviews_dlt():
    return (
        spark.readStream.table("bronze_reviews_dlt")
        .select(
            col("review_id"),
            col("order_id"),
            col("review_score").cast("int").alias("review_score"),
            col("review_comment_title"),
            col("review_comment_message"),
            col("review_creation_date").cast("timestamp_ntz").alias("review_creation_ts"),
            col("review_answer_timestamp").cast("timestamp_ntz").alias("review_answer_ts"),
            col("_ingested_at"),
        )
        # Length derivations — null-safe via TRIM-then-LENGTH, default 0
        .withColumn(
            "title_length",
            when(col("review_comment_title").isNotNull(), length(trim(col("review_comment_title"))))
            .otherwise(lit(0)),
        )
        .withColumn(
            "comment_length",
            when(col("review_comment_message").isNotNull(), length(trim(col("review_comment_message"))))
            .otherwise(lit(0)),
        )
        # UC SQL UDF call — runs in Photon, not Python
        .withColumn(
            "sentiment",
            expr("olist_lakehouse_us.silver.classify_review_sentiment(review_score, comment_length)"),
        )
        .withColumn("_processed_at", current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver_geolocation_dlt` — zip-prefix centroids (materialized view)
# MAGIC
# MAGIC The first materialized view in our pipeline. Worth understanding *why*.
# MAGIC
# MAGIC ### Why this can't be a streaming table
# MAGIC
# MAGIC Bronze geolocation has 1,000,163 rows — multiple lat/lng points per zip
# MAGIC prefix. Silver collapses that to one centroid per zip prefix (~19,010 rows).
# MAGIC That's a many-to-1 aggregation: each output row depends on hundreds of input
# MAGIC rows. Streaming sources are stateless and append-only — a single new Bronze
# MAGIC row would invalidate the centroid for its zip prefix, requiring a recompute
# MAGIC across all rows for that group. Streaming tables don't support that
# MAGIC semantics without watermarks and complex stateful streaming. Materialized
# MAGIC views handle it natively: each pipeline run recomputes the aggregation.
# MAGIC
# MAGIC ### Why `spark.read` (not `spark.readStream`)
# MAGIC
# MAGIC Materialized views must use batch reads. Using `spark.readStream` here would
# MAGIC fail at planning time — SDP knows materialized views are batch and rejects
# MAGIC streaming source references. This pairing rule is enforced by the framework.
# MAGIC
# MAGIC ### Pre-aggregation outlier filter
# MAGIC
# MAGIC Phase 2 found 47 of 1M points falling outside Brazil's bounding box (bad
# MAGIC geocodes from the source). We filter these *before* aggregating — otherwise
# MAGIC a single bad point pulls the centroid off-target. Brazil bounding box from
# MAGIC `data_reference_and_quality.md`: lat in [-33.75, 5.27], lng in [-73.99, -34.80].
# MAGIC
# MAGIC ### MODE() for the city/state
# MAGIC
# MAGIC City and state are non-numeric, so AVG isn't an option. Phase 2 used MODE
# MAGIC (most-frequent value) — for a given zip prefix, return the city/state that
# MAGIC the majority of points label it with. Disambiguates source-side spelling
# MAGIC variants ("sao paulo" vs "são paulo").
# MAGIC
# MAGIC ### `_ingested_at` lineage on an aggregate
# MAGIC
# MAGIC For aggregations, "ingested at" is ambiguous — there are many input rows
# MAGIC with potentially different ingest timestamps. Phase 2 used MIN as the
# MAGIC convention (oldest contributing row); same here.

# COMMAND ----------

@dp.materialized_view(
    name="silver_geolocation_dlt",
    comment=(
        "One centroid per zip code prefix. Aggregated from ~1M Bronze points using "
        "AVG(lat,lng) + MODE(city,state). Outside-bounding-box points filtered "
        "before aggregation."
    ),
    table_properties=SILVER_PROPS,
)
@dp.expect_or_fail("valid_zip_code_prefix", "zip_code_prefix IS NOT NULL")
@dp.expect_all({
    "lat_in_brazil": "lat_centroid BETWEEN -33.75 AND 5.27",
    "lng_in_brazil": "lng_centroid BETWEEN -73.99 AND -34.80",
})
def silver_geolocation_dlt():
    return (
        spark.read.table("bronze_geolocation_dlt")
        .filter(
            "geolocation_lat BETWEEN -33.75 AND 5.27 "
            "AND geolocation_lng BETWEEN -73.99 AND -34.80"
        )
        .groupBy(col("geolocation_zip_code_prefix").alias("zip_code_prefix"))
        .agg(
            spark_round(avg("geolocation_lat"), 6).alias("lat_centroid"),
            spark_round(avg("geolocation_lng"), 6).alias("lng_centroid"),
            mode(col("geolocation_city")).alias("city"),
            mode(col("geolocation_state")).alias("state"),
            count("*").alias("source_point_count"),
            spark_min("_ingested_at").alias("_ingested_at"),
        )
        .withColumn("_processed_at", current_timestamp())
    )
