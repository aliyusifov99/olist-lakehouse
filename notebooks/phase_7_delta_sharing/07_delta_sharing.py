# Databricks notebook source
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Phase 7 — Create the Delta Share
# MAGIC -- Bundles the two Gold tables we want exposed to external consumers.
# MAGIC -- Shares are UC-level securables (sibling to catalogs); they live
# MAGIC -- under the metastore, not under any catalog.
# MAGIC -- ============================================================
# MAGIC
# MAGIC GRANT USE CATALOG ON CATALOG olist_lakehouse_us TO `<YOUR-EMAIL>`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA  olist_lakehouse_us.gold TO `<YOUR-EMAIL>`;
# MAGIC GRANT SELECT      ON SCHEMA  olist_lakehouse_us.gold TO `<YOUR-EMAIL>`;
# MAGIC
# MAGIC CREATE SHARE IF NOT EXISTS olist_analytics_share
# MAGIC   COMMENT 'Olist gold-layer analytics tables shared with partner teams. Aggregated, non-PII (data_classification=internal). Open sharing model — see recipient external_bi_consumer.';
# MAGIC
# MAGIC
# MAGIC -- Add tables to the share. ALTER SHARE ... ADD TABLE is the canonical DDL;
# MAGIC -- you can also REMOVE TABLE later without rebuilding the share.
# MAGIC -- Tables are referenced by their three-level UC name.
# MAGIC ALTER SHARE olist_analytics_share
# MAGIC   ADD TABLE olist_lakehouse_us.gold.monthly_revenue;
# MAGIC
# MAGIC
# MAGIC ALTER SHARE olist_analytics_share
# MAGIC   ADD TABLE olist_lakehouse_us.gold.category_analytics;
# MAGIC
# MAGIC
# MAGIC -- Verify what's inside the share.
# MAGIC SHOW ALL IN SHARE olist_analytics_share;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Phase 7 — Create the Recipient and Grant Access
# MAGIC -- ------------------------------------------------------------
# MAGIC -- ⚠️ FOR REFERENCE ONLY — NOT EXECUTED.
# MAGIC -- This cell documents the open-sharing recipient pattern. The
# MAGIC -- portfolio scope only exercises the share half (cell above);
# MAGIC -- recipient creation was deliberately skipped. See
# MAGIC -- phases_notes_and_plan/phase_7_notes.md for rationale.
# MAGIC -- ------------------------------------------------------------
# MAGIC -- Open-sharing recipient: token-based, for consumers without a
# MAGIC -- Databricks workspace (external BI tools, partners, customers).
# MAGIC -- Databricks issues an activation URL + bearer token bundled
# MAGIC -- into a `config.share` profile file the recipient downloads.
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE RECIPIENT IF NOT EXISTS external_bi_consumer
# MAGIC   COMMENT 'External BI tool / partner without a Databricks workspace. Open sharing model — receives a token-based config.share profile file. For Databricks-to-Databricks sharing, would use USING ID ''<consumer-metastore-id>'' instead.';
# MAGIC
# MAGIC -- Bind the share to the recipient. SELECT is the only privilege
# MAGIC -- Delta Sharing supports — sharing is read-only by design.
# MAGIC GRANT SELECT ON SHARE olist_analytics_share TO RECIPIENT external_bi_consumer;
# MAGIC
# MAGIC
# MAGIC -- ----- Verification -----
# MAGIC
# MAGIC -- Recipient metadata, including the activation_link (single-use URL
# MAGIC -- the recipient uses to download their config.share profile).
# MAGIC DESCRIBE RECIPIENT external_bi_consumer;
# MAGIC
# MAGIC -- Confirm the share-to-recipient binding.
# MAGIC SHOW GRANTS ON SHARE olist_analytics_share;
# MAGIC
# MAGIC -- List all recipients in the metastore.
# MAGIC SHOW RECIPIENTS;