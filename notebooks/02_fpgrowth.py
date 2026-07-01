# Databricks notebook source
# MAGIC %md
# MAGIC # Step 02: Production Basket Affinity Mining
# MAGIC Groups transactional items into baskets, trains FP-Growth association models, and saves model outputs to Delta Lake with target indexing.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth

# Create widgets for configuration
dbutils.widgets.text("min_support", "0.1", "Min Support (0.0 to 1.0)")
dbutils.widgets.text("min_confidence", "0.5", "Min Confidence (0.0 to 1.0)")

min_support = float(dbutils.widgets.get("min_support"))
min_confidence = float(dbutils.widgets.get("min_confidence"))

print(f"Running FP-Growth model with Min Support: {min_support}, Min Confidence: {min_confidence}")

# COMMAND ----------

# Load ingested transaction data
delta_path = "/tmp/ci360/bronze_transactions"
df = spark.read.format("delta").load(delta_path)

# Handle empty data boundary case
if df.count() == 0:
    raise ValueError("Input Bronze table contains no rows. Aborting execution.")

# Aggregate items into arrays per transaction ID (Create shopping baskets)
baskets_df = df.groupBy("transaction_id") \
    .agg(F.collect_list("item").alias("items"))

# Show sample aggregated records
display(baskets_df.limit(10))

# COMMAND ----------

# Configure and train Spark MLlib FP-Growth Model
fp_growth = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
model = fp_growth.fit(baskets_df)

# COMMAND ----------

# Extract and save frequent itemsets (Silver Layer)
freq_itemsets = model.freqItemsets
silver_path = "/tmp/ci360/silver_frequent_itemsets"

print(f"Writing frequent itemsets Delta table to: {silver_path}")
freq_itemsets.write.format("delta") \
    .mode("overwrite") \
    .save(silver_path)

# Optimize Silver Delta Lake Table
spark.sql(f"OPTIMIZE delta.`{silver_path}`")

# COMMAND ----------

# Extract and save association rules (Gold Layer)
# Format Rules: antecedent (LHS) -> consequent (RHS)
association_rules = model.associationRules

# Format rules to support downstream lookups (cast arrays to strings or index column lookup)
# Note: Z-Ordering cannot be run directly on array columns, so we Z-Order by Support / Confidence metrics
gold_path = "/tmp/ci360/gold_association_rules"

print(f"Writing association rules Delta table to: {gold_path}")
association_rules.write.format("delta") \
    .mode("overwrite") \
    .save(gold_path)

# Optimize Gold Delta Lake Table
spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY (confidence, lift)")

print("Basket affinity model computation complete!")
