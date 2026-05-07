# Databricks notebook source
# MAGIC %md
# MAGIC # `02_silver_reviews`
# MAGIC
# MAGIC **Silver Layer:** reviews with sentiment classification + composite-key dedup.
# MAGIC
# MAGIC - **Source:** `olist_lakehouse_us.bronze.reviews` (99,224 rows; 814 duplicate `review_id`s)
# MAGIC - **Target:** `olist_lakehouse_us.silver.reviews`
# MAGIC
# MAGIC ## Transformations applied
# MAGIC
# MAGIC 1. Cast timestamps to `TIMESTAMP_NTZ` (naive Brazil local convention).
# MAGIC 2. Compute `comment_length` and `title_length`.
# MAGIC 3. Apply `silver.classify_review_sentiment` UDF via `F.expr(...)`.
# MAGIC 4. Composite-key dedup on `(review_id, order_id)` — preserves the 814 duplicate review_ids that legitimately span multiple orders.
# MAGIC 5. Defensive full-row dedup via `dropDuplicates` on the composite key.
# MAGIC 6. Add `_processed_at` lineage column.
# MAGIC
# MAGIC ## Why composite key, not single
# MAGIC
# MAGIC Phase 1 found that 814 `review_id` values appear with *different* `order_id`s. Three realistic explanations: source ETL bug, genuine cross-order reviews, or order re-binding after cancellation. Without source-system access we can't tell, so we preserve the data as-is — the natural key is the **pair**, not `review_id` alone. Trade-off: anyone naively running `COUNT(DISTINCT review_id)` will get a number 814 lower than `COUNT(*)`. Documented in the table comment.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, length, trim, current_timestamp, expr

CATALOG = "olist_lakehouse_us"
BRONZE_TABLE = f"{CATALOG}.bronze.reviews"
SILVER_TABLE = f"{CATALOG}.silver.reviews"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform
# MAGIC
# MAGIC Single chained DataFrame transformation. Notes on each step:
# MAGIC
# MAGIC - **`review_score` re-cast** is defensive — Bronze schema hint already typed it as `INT`, but if a future schema evolution introduces a string row, we'd rather see a cast error than silently propagate bad data.
# MAGIC - **`comment_length` / `title_length`** use `COALESCE`-via-`when` to return `0` for missing text rather than `NULL`. The sentiment UDF treats NULL length as zero internally, but storing an explicit `0` is friendlier for downstream consumers.
# MAGIC - **`F.expr(...)`** is the bridge that lets us call our UC SQL UDF from PySpark without losing Photon. See [pyspark.sql.functions.expr](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.expr.html).
# MAGIC - **`dropDuplicates(['review_id', 'order_id'])`** drops only exact composite-key collisions (expected to drop 0 rows). It does **not** dedup on `review_id` alone.

# COMMAND ----------

bronze = spark.table(BRONZE_TABLE)

silver = (
    bronze
    # Defensive type re-assertion
    .withColumn("review_score", col("review_score").cast("int"))
    .withColumn("review_created_at",
                col("review_creation_date").cast("timestamp_ntz"))
    .withColumn("review_answered_at",
                col("review_answer_timestamp").cast("timestamp_ntz"))

    # Derived lengths -- 0 for missing text, not NULL
    .withColumn("comment_length",
                F.when(col("review_comment_message").isNotNull(),
                       length(trim(col("review_comment_message"))))
                 .otherwise(F.lit(0)))
    .withColumn("title_length",
                F.when(col("review_comment_title").isNotNull(),
                       length(trim(col("review_comment_title"))))
                 .otherwise(F.lit(0)))

    # Sentiment via the UC SQL UDF -- runs in Photon
    .withColumn("sentiment",
                expr(f"{CATALOG}.silver.classify_review_sentiment("
                     "review_score, comment_length)"))

    .select(
        "review_id",
        "order_id",
        "review_score",
        "sentiment",
        "review_comment_title",
        "review_comment_message",
        "title_length",
        "comment_length",
        "review_created_at",
        "review_answered_at",
        "_ingested_at",
        current_timestamp().alias("_processed_at"),
    )

    # Composite-key dedup; expected to drop 0 rows
    .dropDuplicates(["review_id", "order_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write
# MAGIC
# MAGIC Standard Silver write pattern: Delta + overwrite + UC-managed location. Table properties (`quality`, `medallion.layer`, `source.timezone`) and the table comment are applied via `ALTER TABLE` / `COMMENT ON TABLE` immediately after creation — cleaner than chaining `.option()` calls on the writer.
# MAGIC
# MAGIC `overwriteSchema=true` lets us re-run the notebook freely if we tweak the schema during development.

# COMMAND ----------

(
    silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE)
)

spark.sql(f"""
    ALTER TABLE {SILVER_TABLE} SET TBLPROPERTIES (
      'quality' = 'silver',
      'medallion.layer' = 'silver',
      'source.timezone' = 'America/Sao_Paulo'
    )
""")

spark.sql(f"""
    COMMENT ON TABLE {SILVER_TABLE} IS
      'Reviews with sentiment classification and length-derived columns. '
      'NOTE: review_id is NOT unique -- 814 review_ids legitimately span '
      'multiple orders. The natural primary key is (review_id, order_id). '
      'Timestamps are naive Brazil local time (America/Sao_Paulo).'
""")

print(f"Wrote {silver.count():,} rows to {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Two checks:
# MAGIC
# MAGIC 1. **Structural** — row count, key uniqueness, null patterns, score average.
# MAGIC 2. **Cross-check sentiment distribution** against the bronze-level run. The percentages should match exactly; any drift signals an unexpected transformation side-effect (e.g., `TRIM` changing a length boundary).
# MAGIC
# MAGIC ### Expected values
# MAGIC
# MAGIC | Metric | Expected |
# MAGIC |---|---|
# MAGIC | `total_rows` | 99,224 |
# MAGIC | `distinct_review_ids` | 98,410 (99,224 − 814) |
# MAGIC | `distinct_review_order_pairs` | 99,224 (composite key is unique) |
# MAGIC | `null_titles` | ~87,656 |
# MAGIC | `null_comments` | ~58,247 |
# MAGIC | `null_scores` | 0 |
# MAGIC | `avg_score` | ~4.0 |

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      COUNT(*)                                AS total_rows,
      COUNT(DISTINCT review_id)               AS distinct_review_ids,
      COUNT(DISTINCT (review_id, order_id))   AS distinct_review_order_pairs,
      SUM(CASE WHEN review_comment_title   IS NULL THEN 1 ELSE 0 END) AS null_titles,
      SUM(CASE WHEN review_comment_message IS NULL THEN 1 ELSE 0 END) AS null_comments,
      SUM(CASE WHEN review_score IS NULL THEN 1 ELSE 0 END)            AS null_scores,
      ROUND(AVG(comment_length), 1)           AS avg_comment_length,
      ROUND(AVG(review_score), 2)             AS avg_score
    FROM {SILVER_TABLE}
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      sentiment,
      COUNT(*) AS review_count,
      ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM {SILVER_TABLE}
    GROUP BY sentiment
    ORDER BY review_count DESC
"""))

# COMMAND ----------

