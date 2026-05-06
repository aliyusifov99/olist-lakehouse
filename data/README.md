# Data — Olist Brazilian E-Commerce Public Dataset

The data files themselves are **not committed to this repository** (see the `.gitignore` rules excluding `data/*.csv`, `data/*.parquet`, `data/*.zip`).

## Source

Brazilian E-Commerce Public Dataset by Olist — Kaggle:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

License: CC BY-NC-SA 4.0 (per the Kaggle dataset page).

## Files

The download contains 9 CSV files (~50 MB total uncompressed):

| File | Rows | Description |
|---|---|---|
| `olist_orders_dataset.csv` | 99,441 | Order header — purchase, approval, delivery timestamps. |
| `olist_order_items_dataset.csv` | 112,650 | Order line items — one row per (order, item-sequence). |
| `olist_order_payments_dataset.csv` | 103,886 | Payment splits — one row per (order, payment-sequential). |
| `olist_order_reviews_dataset.csv` | 100,000 | Review scores and free-text comments. |
| `olist_customers_dataset.csv` | 99,441 | Customer per-order key + customer_unique_id. |
| `olist_products_dataset.csv` | 32,951 | Product catalog with category, dimensions, weight. |
| `olist_sellers_dataset.csv` | 3,095 | Seller catalog with location. |
| `olist_geolocation_dataset.csv` | 1,000,163 | Brazilian zip-code-prefix to lat/long mapping. |
| `product_category_name_translation.csv` | 71 | Category name lookup PT → EN. |

## How to bootstrap a fresh workspace

This project lands the CSVs in a GCS bucket; Databricks Auto Loader picks them up from there (Phase 1).

1. **Download the dataset** from Kaggle (link above). Requires a free Kaggle account.
2. **Unzip** to get the 9 CSVs.
3. **Upload to GCS:**

```bash
   gsutil -m cp *.csv gs://<your-bucket-name>/landing/
```

4. **Run the Phase 1 ingestion notebook** (`notebooks/phase_1/01_bronze_ingestion.py`) which uses Auto Loader to incrementally pick up the files into the `bronze` schema.

## Why CSVs and not the dataset's parquet/json variants

Olist publishes the data only as CSVs on Kaggle. The Phase 1 Bronze layer preserves the source format (with all its typos like `lenght` and the 261,831 geolocation duplicates) and Silver does the cleanup. This is the standard medallion contract — Bronze is faithful to source, Silver is fit-for-purpose.