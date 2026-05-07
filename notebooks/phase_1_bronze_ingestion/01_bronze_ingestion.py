# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC
# MAGIC ## Configuration
# MAGIC
# MAGIC Centralizes paths and identifiers in one place so they're easy to change later
# MAGIC (e.g., when promoting to a prod catalog).
# MAGIC
# MAGIC - **Source data** is governed by external location `olist_raw_landing`.
# MAGIC - **Auto Loader state** lives in a UC-managed volume created in Step 1.1. The
# MAGIC   `/Volumes/<catalog>/<schema>/<volume>/` path is the canonical UC volume URI.
# MAGIC   See [managed volumes docs](https://docs.databricks.com/en/volumes/managed-volumes.html).

# COMMAND ----------

CATALOG = "olist_lakehouse_us"
SCHEMA = "bronze"

RAW_PATH = "gs://gcp-bucket-path/landing"
CHECKPOINT_BASE = "/Volumes/olist_lakehouse_us/bronze/checkpoints"

print(f"Catalog.Schema: {CATALOG}.{SCHEMA}")
print(f"Source:         {RAW_PATH}")
print(f"Checkpoints:    {CHECKPOINT_BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion Stages

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper Function: `ingest_to_bronze`
# MAGIC
# MAGIC Single function that handles ingestion for any source folder. Each call is one
# MAGIC "stream" with its own schema location and checkpoint.
# MAGIC
# MAGIC The function uses `trigger(availableNow=True)` — the stream starts, processes all
# MAGIC currently-available files, then stops. This gives us incremental semantics (only
# MAGIC new files are processed on subsequent runs) without paying for an always-on
# MAGIC streaming cluster.
# MAGIC
# MAGIC Auto Loader is enabled via `cloudFiles` format with directory listing mode (no
# MAGIC file notifications). Schema inference is on, with `addNewColumns` evolution mode.
# MAGIC Each row is enriched with three lineage columns: `_source_file`,
# MAGIC `_file_modified_at`, and `_ingested_at`.

# COMMAND ----------

def ingest_to_bronze(
    source_folder: str,
    target_table: str,
    schema_hints: str = None,
):
    source_path     = f"{RAW_PATH}/{source_folder}/"
    schema_location = f"{CHECKPOINT_BASE}/{target_table}/_schema"
    checkpoint_path = f"{CHECKPOINT_BASE}/{target_table}/_checkpoint"

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
    )

    if schema_hints:
        reader = reader.option("cloudFiles.schemaHints", schema_hints)

    df = reader.load(source_path)

    df_with_metadata = df.selectExpr(
        "*",
        "_metadata.file_path             AS _source_file",
        "_metadata.file_modification_time AS _file_modified_at",
        "current_timestamp()             AS _ingested_at",
    )

    full_table_name = f"{CATALOG}.{SCHEMA}.{target_table}"

    query = (
        df_with_metadata.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(full_table_name)
    )

    query.awaitTermination()

    row_count = spark.table(full_table_name).count()
    print(f"✓ {source_folder:25s} → {full_table_name}  ({row_count:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest `orders`

# COMMAND ----------

ingest_to_bronze(
    source_folder="orders",
    target_table="orders",
    schema_hints=(
        "order_purchase_timestamp       TIMESTAMP, "
        "order_approved_at              TIMESTAMP, "
        "order_delivered_carrier_date   TIMESTAMP, "
        "order_delivered_customer_date  TIMESTAMP, "
        "order_estimated_delivery_date  TIMESTAMP"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify `orders` ingested correctly
# MAGIC
# MAGIC Row count should be 99,441 (per `data_reference_and_quality.md`). Schema check
# MAGIC confirms timestamps as `TIMESTAMP` and metadata columns are present.

# COMMAND ----------

print(f"Row count: {spark.table('olist_lakehouse_us.bronze.orders').count():,}")

spark.table("olist_lakehouse_us.bronze.orders").printSchema()

display(
    spark.table("olist_lakehouse_us.bronze.orders")
    .select(
        "order_id",
        "order_status",
        "order_purchase_timestamp",
        "order_estimated_delivery_date",
        "_source_file",
        "_file_modified_at",
        "_ingested_at",
    )
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest `order_items`

# COMMAND ----------

ingest_to_bronze(
    source_folder="order_items",
    target_table="order_items",
    schema_hints=(
        "shipping_limit_date  TIMESTAMP, "
        "price                DOUBLE, "
        "freight_value        DOUBLE"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest `payments`

# COMMAND ----------

ingest_to_bronze(
    source_folder="payments",
    target_table="payments",
    schema_hints=(
        "payment_sequential   INT, "
        "payment_installments INT, "
        "payment_value        DOUBLE"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest `reviews`

# COMMAND ----------

ingest_to_bronze(
    source_folder="reviews",
    target_table="reviews",
    schema_hints=(
        "review_score             INT, "
        "review_creation_date     TIMESTAMP, "
        "review_answer_timestamp  TIMESTAMP"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest `products`

# COMMAND ----------

ingest_to_bronze(
    source_folder="products",
    target_table="products",
    schema_hints=(
        "product_name_lenght        INT, "
        "product_description_lenght INT, "
        "product_photos_qty         INT, "
        "product_weight_g           DOUBLE, "
        "product_length_cm          DOUBLE, "
        "product_height_cm          DOUBLE, "
        "product_width_cm           DOUBLE"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest `customers`

# COMMAND ----------

ingest_to_bronze(
    source_folder="customers",
    target_table="customers",
    schema_hints="customer_zip_code_prefix STRING",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest `sellers`

# COMMAND ----------

ingest_to_bronze(
    source_folder="sellers",
    target_table="sellers",
    schema_hints="seller_zip_code_prefix STRING",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest `geolocation`

# COMMAND ----------

ingest_to_bronze(
    source_folder="geolocation",
    target_table="geolocation",
    schema_hints=(
        "geolocation_zip_code_prefix STRING, "
        "geolocation_lat             DOUBLE, "
        "geolocation_lng             DOUBLE"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest `category_translation`

# COMMAND ----------

ingest_to_bronze(
    source_folder="category_translation",
    target_table="category_translation",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC ### Final tally — compare against expected row counts

# COMMAND ----------

expected = {
    "orders":               99_441,
    "order_items":         112_650,
    "payments":            103_886,
    "reviews":              99_224,
    "products":             32_951,
    "customers":            99_441,
    "sellers":               3_095,
    "geolocation":       1_000_163,
    "category_translation":     71,
}

print(f"{'Table':<25} {'Actual':>12} {'Expected':>12} {'Status':>8}")
print("-" * 60)
for table, exp in expected.items():
    actual = spark.table(f"olist_lakehouse_us.bronze.{table}").count()
    status = "✓" if actual == exp else "✗"
    print(f"{table:<25} {actual:>12,} {exp:>12,} {status:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema audit
# MAGIC
# MAGIC Print each table's schema for visual inspection. Things to verify:
# MAGIC
# MAGIC - All hinted columns have the type we asked for (`TIMESTAMP`, `DOUBLE`, etc.)
# MAGIC - Zip code prefixes are `STRING` (not `INT` — would lose leading zeros)
# MAGIC - Metadata columns present everywhere
# MAGIC - No surprise types (e.g., a column you expected as `INT` showing as `STRING`)

# COMMAND ----------

tables = [
    "orders", "order_items", "payments", "reviews",
    "products", "customers", "sellers", "geolocation",
    "category_translation",
]

for t in tables:
    print(f"\n{'='*60}")
    print(f"  {t}")
    print('='*60)
    spark.table(f"olist_lakehouse_us.bronze.{t}").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rescued data check
# MAGIC
# MAGIC `_rescued_data` is a `STRING` column Auto Loader adds automatically. It captures
# MAGIC any field values that couldn't be parsed into the inferred schema — for example,
# MAGIC if a row had `"abc"` in a column we typed as `INT`, the row still gets ingested
# MAGIC but the malformed values land in `_rescued_data` as JSON instead of being dropped.
# MAGIC
# MAGIC This is the **"rescue, don't lose"** philosophy: malformed rows aren't silently
# MAGIC discarded, they're flagged for inspection.
# MAGIC
# MAGIC A non-zero count here is **not necessarily an error** — it just means something
# MAGIC was unparseable. We then have to decide: fix the schema, accept the loss, or
# MAGIC handle in Silver.
# MAGIC
# MAGIC Reference: [rescued data column docs](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/schema.html#what-is-the-rescued-data-column).

# COMMAND ----------

print(f"{'Table':<25} {'Rescued rows':>15}")
print("-" * 42)
for t in tables:
    n = (
        spark.table(f"olist_lakehouse_us.bronze.{t}")
        .filter("_rescued_data IS NOT NULL")
        .count()
    )
    print(f"{t:<25} {n:>15,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata column completeness
# MAGIC
# MAGIC Every Bronze row should have all three metadata columns populated. If any are
# MAGIC null, something is wrong with the helper function or Auto Loader's `_metadata`
# MAGIC access.

# COMMAND ----------

from pyspark.sql.functions import col, count, when

for t in tables:
    df = spark.table(f"olist_lakehouse_us.bronze.{t}")
    null_counts = df.select([
        count(when(col("_source_file").isNull(),       1)).alias("null_source_file"),
        count(when(col("_file_modified_at").isNull(),  1)).alias("null_file_modified"),
        count(when(col("_ingested_at").isNull(),       1)).alias("null_ingested_at"),
    ]).collect()[0]

    issues = []
    if null_counts["null_source_file"]:    issues.append(f"source_file={null_counts['null_source_file']}")
    if null_counts["null_file_modified"]:  issues.append(f"file_modified={null_counts['null_file_modified']}")
    if null_counts["null_ingested_at"]:    issues.append(f"ingested_at={null_counts['null_ingested_at']}")

    status = "✓" if not issues else f"✗ {', '.join(issues)}"
    print(f"{t:<25} {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distinct source files per table

# COMMAND ----------

from pyspark.sql.functions import countDistinct

for t in tables:
    n_files = (
        spark.table(f"olist_lakehouse_us.bronze.{t}")
        .select(countDistinct("_source_file").alias("n"))
        .collect()[0]["n"]
    )
    print(f"{t:<25} {n_files} source file(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta history for `orders`
# MAGIC
# MAGIC Proves Delta Lake is working — each commit is recorded with operation metadata.

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY olist_lakehouse_us.bronze.orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader on `payments` — Incremental Ingestion Demo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reset `payments` to ground zero (DEV-ONLY)
# MAGIC
# MAGIC Drops the Delta table **and** wipes Auto Loader's checkpoint+schema state. Both
# MAGIC must go together: the checkpoint's RocksDB store is what tells Auto Loader
# MAGIC *"I've already processed `olist_order_payments_dataset.csv`."* Without clearing
# MAGIC it, Auto Loader would skip everything as "seen" even after the table is dropped.
# MAGIC
# MAGIC > ⚠️ This is a **dev-only** operation. In production, dropping a Bronze table
# MAGIC > breaks every downstream layer.

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS olist_lakehouse_us.bronze.payments")
# dbutils.fs.rm("/Volumes/olist_lakehouse_us/bronze/checkpoints/payments/", True)

# print("Table dropped, checkpoint cleared.")
# print("Confirm GCS source folder is empty:")
# display(dbutils.fs.ls("gs://gcp-bucket-path/landing/payments/"))

# COMMAND ----------

# Confirm only batch1 is in GCS
display(dbutils.fs.ls("gs://gcp-bucket-path/landing/payments/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run ingestion in two stages
# MAGIC
# MAGIC Split the payments table into two batches: upload the first to GCS and run
# MAGIC ingestion, then upload the second and re-run. Sanity check: ~51,943 rows after
# MAGIC stage 1 and 103,886 rows after stage 2.

# COMMAND ----------

print(f"\nRow count after stage 1: "
      f"{spark.table('olist_lakehouse_us.bronze.payments').count():,}")

# Confirm only batch1's path appears in _source_file
display(
    spark.table("olist_lakehouse_us.bronze.payments")
    .groupBy("_source_file").count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Per-file lineage and ingestion timing
# MAGIC
# MAGIC This is the headline screenshot for incremental ingestion: two distinct source
# MAGIC files contributed rows, ingested at different points in time.
# MAGIC
# MAGIC > **Caption material:** "Auto Loader incrementally ingested two batches into one
# MAGIC > Delta table, preserving file-level lineage and ingestion timestamps."

# COMMAND ----------

from pyspark.sql.functions import min as F_min, max as F_max, count as F_count

display(
    spark.table("olist_lakehouse_us.bronze.payments")
    .groupBy("_source_file")
    .agg(
        F_count("*").alias("rows"),
        F_min("_ingested_at").alias("first_ingested"),
        F_max("_ingested_at").alias("last_ingested"),
    )
    .orderBy("first_ingested")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Two separate `STREAMING UPDATE` operations in the Delta log
# MAGIC
# MAGIC Each `ingest_to_bronze` call → one Delta version (atomic commit).
# MAGIC `operationMetrics.numOutputRows` on each version is irrefutable evidence of
# MAGIC incremental behavior — exactly the size of one batch each.

# COMMAND ----------

display(
    spark.sql("""
        SELECT
          version,
          timestamp,
          operation,
          operationMetrics.numOutputRows AS rows_added,
          operationMetrics.numAddedFiles AS files_added
        FROM (DESCRIBE HISTORY olist_lakehouse_us.bronze.payments)
        ORDER BY version DESC
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time travel
# MAGIC
# MAGIC Delta time travel: query the table as it existed at any prior version. This is
# MAGIC the "audit trail" feature that makes Delta Lake more than just Parquet-with-ACID.

# COMMAND ----------

# Find the version corresponding to the end of stage 1
stage1_version = (
    spark.sql("DESCRIBE HISTORY olist_lakehouse_us.bronze.payments")
    .filter("operation = 'STREAMING UPDATE'")
    .orderBy("version")
    .first()["version"]
)
print(f"Stage 1 ended at table version {stage1_version}")

# Query the table AS OF that version — should show only batch1's rows
display(spark.sql(f"""
    SELECT COUNT(*) AS rows_at_stage1
    FROM olist_lakehouse_us.bronze.payments VERSION AS OF {stage1_version}
"""))

# Current state — both batches
display(spark.sql("""
    SELECT COUNT(*) AS rows_now
    FROM olist_lakehouse_us.bronze.payments
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Features (optional, for learning purposes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `DESCRIBE EXTENDED`: full table metadata
# MAGIC
# MAGIC Shows column schema, table properties, location, table format, ownership,
# MAGIC comments, and constraints in one view. The standard *"what is this table?"*
# MAGIC query for Unity Catalog tables.
# MAGIC
# MAGIC Reference: [DESCRIBE TABLE docs](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html).

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED olist_lakehouse_us.bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ### `OPTIMIZE`: compact small files into larger ones
# MAGIC
# MAGIC The "small files problem" hurts query performance because Spark has per-file
# MAGIC overhead for opening, reading metadata, and listing. `OPTIMIZE` reads small
# MAGIC files, combines their contents, and writes back as fewer larger files. Default
# MAGIC target size is 1 GB per file.
# MAGIC
# MAGIC For our 99K-row `orders` table this is essentially a no-op (we already have 1–2
# MAGIC files), but the *command* is what we're demonstrating. In a real pipeline that
# MAGIC ingests micro-batches every minute, `OPTIMIZE` is run nightly to compact the
# MAGIC day's accumulated small files.
# MAGIC
# MAGIC Reference: [OPTIMIZE docs](https://docs.databricks.com/en/delta/optimize.html).

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE olist_lakehouse_us.bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ### `Z-ORDER`: cluster data by frequently-filtered columns
# MAGIC
# MAGIC One of the highest-impact Delta Lake features for query performance. Mechanism:
# MAGIC
# MAGIC - Delta stores per-file min/max statistics for every column.
# MAGIC - When you query `WHERE order_purchase_timestamp BETWEEN ...`, Spark can **skip
# MAGIC   entire Parquet files** whose stats don't overlap.
# MAGIC - But for skipping to work, related rows need to be physically co-located in the
# MAGIC   same files.
# MAGIC - `Z-ORDER` rearranges files so values that are "close" in the Z-ORDER columns
# MAGIC   end up in the same files.
# MAGIC - More files skipped → fewer bytes read → faster queries.
# MAGIC
# MAGIC **Cost:** it's a full rewrite of the table. Run during maintenance windows, not
# MAGIC on every ingest.
# MAGIC
# MAGIC We Z-ORDER on `order_purchase_timestamp` because nearly every analytical query
# MAGIC on orders filters by date range (monthly revenue, RFM recency, delivery trends).
# MAGIC State and status are also good candidates, but you can only effectively Z-ORDER
# MAGIC on 2–4 columns at most — too many and the benefit dilutes.
# MAGIC
# MAGIC Reference: [data skipping docs](https://docs.databricks.com/en/delta/data-skipping.html).
# MAGIC As of late 2024, *liquid clustering* is the newer alternative.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE olist_lakehouse_us.bronze.orders
# MAGIC   ZORDER BY (order_purchase_timestamp);

# COMMAND ----------

# MAGIC %md
# MAGIC ### `VACUUM`: physically delete files no longer needed by current Delta version
# MAGIC
# MAGIC When `OPTIMIZE`/`UPDATE`/`DELETE`/`MERGE` rewrites files, the old files don't
# MAGIC get physically deleted immediately — they're just marked unreferenced in the
# MAGIC Delta log. They stay around to support time travel.
# MAGIC
# MAGIC `VACUUM` is what physically deletes them. It only deletes files older than the
# MAGIC retention threshold (default: 7 days = 168 hours).
# MAGIC
# MAGIC **Why the 7-day default:** it's the safety threshold for time travel and
# MAGIC in-flight readers. If you `VACUUM` more aggressively, you can corrupt a
# MAGIC long-running query that started before `VACUUM` ran. There's a safeguard:
# MAGIC `VACUUM` with retention < 168h **fails** unless you explicitly override
# MAGIC `delta.retentionDurationCheck.enabled`.
# MAGIC
# MAGIC We do a **dry run** here — shows what *would* be deleted without actually
# MAGIC deleting anything. Safer for demonstration.
# MAGIC
# MAGIC Reference: [VACUUM docs](https://docs.databricks.com/en/delta/vacuum.html).

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM olist_lakehouse_us.bronze.orders DRY RUN;
