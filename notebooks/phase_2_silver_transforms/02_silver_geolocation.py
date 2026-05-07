# Databricks notebook source
# MAGIC %md
# MAGIC # `02_silver_geolocation`
# MAGIC
# MAGIC **Silver Layer:** zip-prefix-level geocode lookup.
# MAGIC
# MAGIC - **Source:** `olist_lakehouse_us.bronze.geolocation` (1,000,163 rows; 261,831 full-row duplicates; multiple lat/lng per zip)
# MAGIC - **Target:** `olist_lakehouse_us.silver.geolocation` (~19K rows expected — one per zip prefix)
# MAGIC
# MAGIC ## Transformations applied
# MAGIC
# MAGIC 1. Filter to Brazil's bounding box: `-33.75 ≤ lat ≤ 5.27`, `-73.99 ≤ lng ≤ -34.80`. Discards geocoding errors.
# MAGIC 2. Normalize zip prefix to 5-char string with leading zeros.
# MAGIC 3. Normalize city: `TRIM`, collapse multi-spaces, lowercase.
# MAGIC 4. Aggregate per zip prefix: `AVG` for lat/lng centroid, `MODE` for city/state.
# MAGIC 5. Add `_processed_at` lineage column.
# MAGIC
# MAGIC ## Why aggregate to one-row-per-zip in Silver
# MAGIC
# MAGIC Bronze has ~1M address-level points; downstream joins to `customers` and `sellers` are by `zip_code_prefix`, and each customer/seller resolves to one location. Keeping multiple geocodes per zip in Silver would force every Gold query to either dedupe-at-join-time or risk silent fan-out.
# MAGIC
# MAGIC This is a *technical normalization* (collapse duplicate keys to a unique key with a deterministic centroid), not a *business aggregation*. The raw point-level data stays in Bronze if we ever need it.
# MAGIC
# MAGIC ## Why filter bounding box BEFORE aggregating
# MAGIC
# MAGIC If we averaged first, a bad geocode (e.g., a point in Antarctica) would pull the centroid. Filter, then aggregate.
# MAGIC
# MAGIC ## City/state via MODE
# MAGIC
# MAGIC A 5-digit zip prefix can span multiple cities. `mode()` picks the most-frequent value. Spark 3.4+ supports `mode(col)` as an aggregate. See [mode() docs](https://docs.databricks.com/en/sql/language-manual/functions/mode.html).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.silver.geolocation
# MAGIC USING DELTA
# MAGIC COMMENT 'Zip-prefix-level geocode lookup. One row per geolocation_zip_code_prefix. '
# MAGIC         'Centroid (avg lat/lng) computed across all in-bounds points; city/state '
# MAGIC         'taken as the mode (most frequent value) for that prefix. Source point-level '
# MAGIC         'data preserved in bronze.geolocation.'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'silver',
# MAGIC   'medallion.layer' = 'silver'
# MAGIC )
# MAGIC AS
# MAGIC WITH cleaned AS (
# MAGIC   -- Stage 1: filter bounding box, normalize zip and city
# MAGIC   SELECT
# MAGIC     LPAD(CAST(geolocation_zip_code_prefix AS STRING), 5, '0') AS zip_code_prefix,
# MAGIC     geolocation_lat AS lat,
# MAGIC     geolocation_lng AS lng,
# MAGIC     LOWER(REGEXP_REPLACE(TRIM(geolocation_city), '\\s+', ' ')) AS city,
# MAGIC     geolocation_state AS state,
# MAGIC     _ingested_at
# MAGIC   FROM olist_lakehouse_us.bronze.geolocation
# MAGIC   WHERE geolocation_lat  BETWEEN -33.75 AND 5.27
# MAGIC     AND geolocation_lng  BETWEEN -73.99 AND -34.80
# MAGIC     AND geolocation_zip_code_prefix IS NOT NULL
# MAGIC )
# MAGIC -- Stage 2: aggregate to one row per zip prefix
# MAGIC SELECT
# MAGIC   zip_code_prefix,
# MAGIC   ROUND(AVG(lat), 6)        AS lat_centroid,
# MAGIC   ROUND(AVG(lng), 6)        AS lng_centroid,
# MAGIC   MODE(city)                AS city,
# MAGIC   MODE(state)               AS state,
# MAGIC   COUNT(*)                  AS source_point_count,
# MAGIC   MIN(_ingested_at)         AS _ingested_at,
# MAGIC   CURRENT_TIMESTAMP()       AS _processed_at
# MAGIC FROM cleaned
# MAGIC GROUP BY zip_code_prefix;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Four checks:
# MAGIC
# MAGIC 1. **Structural** — row count, key uniqueness, null patterns.
# MAGIC 2. **Filter accounting** — how many points fell outside the bounding box.
# MAGIC 3. **Centroid sanity** — avg lat/lng land in plausible Brazil range.
# MAGIC 4. **Coverage** — verify zip prefixes used by customers/sellers are present in Silver geolocation. Phase 1 warned: *"Not every zip prefix used in customers/sellers is guaranteed to exist here."*
# MAGIC
# MAGIC ### Expected values
# MAGIC
# MAGIC | Metric | Expected |
# MAGIC |---|---|
# MAGIC | `total_rows` | ~19,000 (one per zip prefix) |
# MAGIC | `distinct_zip_prefixes` | = total_rows (key is unique) |
# MAGIC | `distinct_states` | ≤27 |
# MAGIC | `bronze_total` | 1,000,163 |
# MAGIC | `bronze_outside_box` | small (<1% of bronze) |
# MAGIC | `customers_with_no_geo_match` | small but non-zero (Phase 1 warning) |
# MAGIC | `sellers_with_no_geo_match` | small but non-zero |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                AS total_rows,
# MAGIC   COUNT(DISTINCT zip_code_prefix)         AS distinct_zip_prefixes,
# MAGIC   COUNT(DISTINCT state)                   AS distinct_states,
# MAGIC   ROUND(AVG(lat_centroid), 4)             AS avg_lat,
# MAGIC   ROUND(AVG(lng_centroid), 4)             AS avg_lng,
# MAGIC   MIN(lat_centroid)                       AS min_lat,
# MAGIC   MAX(lat_centroid)                       AS max_lat,
# MAGIC   MIN(lng_centroid)                       AS min_lng,
# MAGIC   MAX(lng_centroid)                       AS max_lng,
# MAGIC   ROUND(AVG(source_point_count), 1)       AS avg_points_per_zip,
# MAGIC   MAX(source_point_count)                 AS max_points_per_zip
# MAGIC FROM olist_lakehouse_us.silver.geolocation;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many bronze points were filtered out by the bounding box?
# MAGIC SELECT
# MAGIC   COUNT(*)                                                       AS bronze_total,
# MAGIC   SUM(CASE
# MAGIC         WHEN geolocation_lat NOT BETWEEN -33.75 AND 5.27
# MAGIC           OR geolocation_lng NOT BETWEEN -73.99 AND -34.80
# MAGIC         THEN 1 ELSE 0
# MAGIC       END)                                                       AS outside_box,
# MAGIC   ROUND(100.0 * SUM(CASE
# MAGIC         WHEN geolocation_lat NOT BETWEEN -33.75 AND 5.27
# MAGIC           OR geolocation_lng NOT BETWEEN -73.99 AND -34.80
# MAGIC         THEN 1 ELSE 0 END) / COUNT(*), 4)                        AS outside_box_pct
# MAGIC FROM olist_lakehouse_us.bronze.geolocation;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Are all customer and seller zip prefixes present in silver.geolocation?
# MAGIC -- Phase 1 warned this is not guaranteed.
# MAGIC SELECT
# MAGIC   'customers' AS entity,
# MAGIC   COUNT(DISTINCT c.customer_zip_code_prefix)                    AS distinct_zips_used,
# MAGIC   COUNT(DISTINCT CASE WHEN g.zip_code_prefix IS NULL
# MAGIC                       THEN c.customer_zip_code_prefix END)      AS zips_with_no_geo_match
# MAGIC FROM olist_lakehouse_us.silver.customers c
# MAGIC LEFT JOIN olist_lakehouse_us.silver.geolocation g
# MAGIC   ON c.customer_zip_code_prefix = g.zip_code_prefix
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'sellers' AS entity,
# MAGIC   COUNT(DISTINCT s.seller_zip_code_prefix)                      AS distinct_zips_used,
# MAGIC   COUNT(DISTINCT CASE WHEN g.zip_code_prefix IS NULL
# MAGIC                       THEN s.seller_zip_code_prefix END)        AS zips_with_no_geo_match
# MAGIC FROM olist_lakehouse_us.silver.sellers s
# MAGIC LEFT JOIN olist_lakehouse_us.silver.geolocation g
# MAGIC   ON s.seller_zip_code_prefix = g.zip_code_prefix;

# COMMAND ----------

