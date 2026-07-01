# Databricks notebook source
# MAGIC %md
# MAGIC # Step 01: Production Ingestion
# MAGIC Ingests transactional log files into Delta Lake. Implements production-grade schema validation and Delta optimizations.

# COMMAND ----------

import os
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F

# COMMAND ----------

# Create parameters/widgets for production configurability
# NOTE: default points to a Unity Catalog Volume path, not a local drive.
# databricks.yml overrides this per-environment (dev/prod), but the default
# itself must also be a path that resolves on a Databricks cluster.
dbutils.widgets.text("data_path", "/Volumes/main/ci360/raw_data/transactions.csv", "Raw CSV Data Path")
dbutils.widgets.text("output_table", "bronze_transactions", "Bronze Delta Table Name")

data_path = dbutils.widgets.get("data_path")
output_table = dbutils.widgets.get("output_table")

# Fail fast with a clear error instead of a confusing Spark stack trace
# if someone runs this notebook standalone with a bad/local path.
if data_path.lower().startswith(("d:", "c:", "/mnt/user-data", "./", "../")) or ":\\" in data_path:
    raise ValueError(
        f"data_path '{data_path}' looks like a local filesystem path, not a "
        "Databricks-accessible path (expected dbfs:/... or /Volumes/...). "
        "Set the 'data_path' widget or the databricks.yml base_parameters."
    )

print(f"Ingesting raw CSV files from: {data_path}")
print(f"Targeting table output: {output_table}")

# COMMAND ----------

# Define strict data schema
schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("item", StringType(), False)
])

# Read transaction log CSV
df = spark.read.format("csv") \
    .option("header", "True") \
    .schema(schema) \
    .load(data_path)

# Verify schema layout
df.printSchema()

# COMMAND ----------

# Production Optimizations: Save as optimized Delta Table
# We enable Change Data Feed (CDF) for downstream incremental processing
# We Z-Order by transaction_id to optimize search lookups

delta_path = f"/tmp/ci360/{output_table}"
print(f"Writing Delta files to storage at: {delta_path}")

df.write.format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .save(delta_path)

# Run SQL optimization to speed up Spark scans
spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY (transaction_id)")

print(f"Ingestion successful! Delta Lake table created at: {delta_path}")
