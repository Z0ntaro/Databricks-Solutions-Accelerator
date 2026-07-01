# Databricks notebook source
# MAGIC %md
# MAGIC # Step 03: Production Recommendation Engine
# MAGIC Exposes a structured API query function to retrieve cross-sell suggestions from association tables.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Load Gold Association Rules delta storage
gold_path = "/tmp/ci360/gold_association_rules"
try:
    rules_df = spark.read.format("delta").load(gold_path)
except Exception as e:
    raise IOError(f"Failed to load Gold Association Table. Ensure notebook 02 ran successfully. Details: {str(e)}")

# COMMAND ----------

def get_recommendations(purchase_items, limit=5):
    """
    Given a list of purchased items (antecedents), queries the Gold association table
    to return top corresponding cross-sell products sorted by confidence.

    Matching is order-insensitive: ["laptop","bag"] and ["bag","laptop"] both match
    the same stored rule, because both sides are sorted before comparison.
    """
    # Parameter validations
    if not isinstance(purchase_items, list):
        raise TypeError("Input 'purchase_items' must be a Python list of strings.")

    if len(purchase_items) == 0:
        return spark.createDataFrame([], rules_df.schema)

    print(f"Querying recommendations for antecedent: {purchase_items}")

    # Sort the input so comparison doesn't depend on the order items were purchased in
    sorted_input = sorted(purchase_items)

    # Run Spark SQL query lookup
    # Sort the stored antecedent array the same way before comparing, so
    # ["laptop","bag"] and ["bag","laptop"] both match the same rule.
    res_df = rules_df.filter(F.array_sort(F.col("antecedent")) == F.array(*[F.lit(x) for x in sorted_input])) \
        .sort(F.col("confidence").desc()) \
        .limit(limit)

    return res_df

# COMMAND ----------

# Production Demo 1: Retail Checkout Cross-Sell
# A shopper purchases a laptop
print("Demo 1: Shopper buys laptop")
laptop_recs = get_recommendations(["laptop"])
display(laptop_recs)

# COMMAND ----------

# Production Demo 2: FSI Card Reward Matching
# A cardholder buys a credit_card product
print("Demo 2: Cardholder acquires credit_card")
card_recs = get_recommendations(["credit_card"])
display(card_recs)
