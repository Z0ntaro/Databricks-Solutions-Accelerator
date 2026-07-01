# Databricks notebook source
# MAGIC %md
# MAGIC # CI-360: Campaign Effectiveness & Basket Analysis Solution Accelerator
# MAGIC Attach this notebook to a Databricks cluster and hit **Run All** to deploy and run the entire ingestion, training, and recommendation pipeline.

# COMMAND ----------

import time

# Start execution telemetry
start_time = time.time()
print("Starting CI-360 production pipeline execution...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Ingest Transaction Logs (Bronze Layer)

# COMMAND ----------

# MAGIC %run ./notebooks/01_ingest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Compute FP-Growth Basket Affinities (Silver & Gold Layers)

# COMMAND ----------

# MAGIC %run ./notebooks/02_fpgrowth

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Serve Cross-Sell Recommendations

# COMMAND ----------

# MAGIC %run ./notebooks/03_recommend

# COMMAND ----------

# End telemetry
elapsed_time = time.time() - start_time
print(f"CI-360 pipeline run completed successfully in {elapsed_time:.2f} seconds!")
