# Databricks notebook source
files = spark.sql("LIST 'gs://gcp-bucket-path/landing/orders/'")
display(files)

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN olist_lakehouse_us").show()

# COMMAND ----------

spark.sql("""
  CREATE TABLE IF NOT EXISTS olist_lakehouse_us.staging.test_table 
  (id INT, name STRING) USING DELTA
""")
spark.sql("INSERT INTO olist_lakehouse_us.staging.test_table VALUES (1, 'setup_test')")
display(spark.sql("SELECT * FROM olist_lakehouse_us.staging.test_table"))
spark.sql("DROP TABLE olist_lakehouse_us.staging.test_table")
print("✓ All checks passed — environment is ready!")