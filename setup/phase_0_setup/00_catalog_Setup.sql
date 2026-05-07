USE CATALOG olist_lakehouse_us; 

CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw ingested data from GCS - no transformations applied';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Cleaned, validated, and enriched data';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Business-ready aggregated tables for analytics';

CREATE SCHEMA IF NOT EXISTS staging
  COMMENT 'Staging area for intermediate processing and testing';