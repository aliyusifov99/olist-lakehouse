# Databricks notebook source
# MAGIC %md
# MAGIC # `02_silver_products`
# MAGIC
# MAGIC **Silver Layer:** products with English category names + derived dimensions.
# MAGIC
# MAGIC - **Sources:**
# MAGIC   - `olist_lakehouse_us.bronze.products` (32,951 rows)
# MAGIC   - `olist_lakehouse_us.bronze.category_translation` (71 rows)
# MAGIC - **Target:** `olist_lakehouse_us.silver.products`
# MAGIC
# MAGIC ## Transformations applied
# MAGIC
# MAGIC 1. Fix source typos: `product_name_lenght` → `product_name_length` (same for description).
# MAGIC 2. Join Portuguese category name to the English translation table.
# MAGIC 3. Hand-translate the 2 known untranslated categories (`pc_gamer`,
# MAGIC    `portateis_cozinha_e_preparadores_de_alimentos`).
# MAGIC 4. Coalesce missing categories to `'unknown'` (610 rows have NULL category in Bronze).
# MAGIC 5. Derive `product_volume_cm3` from dimensions.
# MAGIC 6. Add `_processed_at` lineage column.
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - We keep the Portuguese category as `category_name_pt` for audit / bilingual analysis.
# MAGIC - `product_volume_cm3` is `NULL` if any dimension is missing (2 products have all
# MAGIC   dims null per Phase 1 data-quality notes).
# MAGIC - The outer `COALESCE` falls back to `'unknown'` for any future untranslated value.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_lakehouse_us.silver.products
# MAGIC USING DELTA
# MAGIC COMMENT 'Products with English category names, fixed schema (lenght -> length), '
# MAGIC         'and derived volume. Untranslated categories hand-mapped; null categories '
# MAGIC         'coalesced to "unknown".'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'silver',
# MAGIC   'medallion.layer' = 'silver'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   p.product_id,
# MAGIC
# MAGIC   p.product_category_name AS category_name_pt,
# MAGIC
# MAGIC   COALESCE(
# MAGIC     t.product_category_name_english,
# MAGIC     CASE p.product_category_name
# MAGIC       WHEN 'pc_gamer' THEN 'pc_gaming'
# MAGIC       WHEN 'portateis_cozinha_e_preparadores_de_alimentos'
# MAGIC         THEN 'portable_kitchen_food_processors'
# MAGIC       ELSE NULL
# MAGIC     END,
# MAGIC     'unknown'
# MAGIC   ) AS category_name_en,
# MAGIC
# MAGIC   p.product_weight_g,
# MAGIC   p.product_length_cm,
# MAGIC   p.product_height_cm,
# MAGIC   p.product_width_cm,
# MAGIC
# MAGIC   ROUND(p.product_length_cm * p.product_height_cm * p.product_width_cm, 2)
# MAGIC     AS product_volume_cm3,
# MAGIC
# MAGIC   -- Fix source typos: lenght -> length
# MAGIC   p.product_name_lenght        AS product_name_length,
# MAGIC   p.product_description_lenght AS product_description_length,
# MAGIC   p.product_photos_qty,
# MAGIC
# MAGIC   p._ingested_at,
# MAGIC   CURRENT_TIMESTAMP() AS _processed_at
# MAGIC
# MAGIC FROM olist_lakehouse_us.bronze.products p
# MAGIC LEFT JOIN olist_lakehouse_us.bronze.category_translation t
# MAGIC   ON p.product_category_name = t.product_category_name
# MAGIC WHERE p.product_id IS NOT NULL;  -- Silver contract: no null PKs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Row counts, category coverage, and dimension nulls.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                AS total_rows,
# MAGIC   COUNT(DISTINCT product_id)                              AS distinct_product_ids,
# MAGIC   SUM(CASE WHEN category_name_en = 'unknown' THEN 1 ELSE 0 END) AS unknown_category_rows,
# MAGIC   SUM(CASE WHEN category_name_en = 'pc_gaming' THEN 1 ELSE 0 END) AS pc_gamer_rows,
# MAGIC   SUM(CASE WHEN category_name_en = 'portable_kitchen_food_processors' THEN 1 ELSE 0 END)
# MAGIC     AS portateis_rows,
# MAGIC   SUM(CASE WHEN product_volume_cm3 IS NULL THEN 1 ELSE 0 END) AS null_volume_rows,
# MAGIC   COUNT(DISTINCT category_name_en)                        AS distinct_categories_en
# MAGIC FROM olist_lakehouse_us.silver.products;

# COMMAND ----------

