-- Databricks notebook source
-- Check whether the catalog has a managed storage root configured.
-- If storage_root is non-null, we can create managed volumes here.
DESCRIBE CATALOG EXTENDED olist_lakehouse_us;

-- COMMAND ----------

-- Create a managed volume in the bronze schema for Auto Loader state.
-- Managed volume = UC-governed, storage handled automatically, no external
-- location config needed. Reference:
-- https://docs.databricks.com/en/volumes/managed-volumes.html
CREATE VOLUME IF NOT EXISTS olist_lakehouse_us.bronze.checkpoints
  COMMENT 'Auto Loader schema and checkpoint state for bronze ingestion';

-- Verify it was created
DESCRIBE VOLUME olist_lakehouse_us.bronze.checkpoints;