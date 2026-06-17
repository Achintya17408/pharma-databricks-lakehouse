# Databricks notebook source
# Cell 1
from pyspark.sql import functions as F
from pyspark.sql.window import Window
silver = spark.table("cms_lakehouse.silver.partd_clean")

# Cell 2: State totals for market share denominator
state_totals = silver.groupBy("year", "state").agg(F.sum("total_drug_cost").alias("state_total_cost"))

# Cell 3: Drug-state-year aggregation
drug_state = silver.groupBy("year", "state", "generic_name").agg(
    F.sum("total_claims").alias("claims"),
    F.sum("total_drug_cost").alias("drug_cost"),
    F.countDistinct("provider_npi").alias("prescribers")
)

# Cell 4: Add market share + YoY growth
market = drug_state.join(state_totals, ["year", "state"], "left")
market = market.withColumn("market_share", F.col("drug_cost") / F.col("state_total_cost"))
w = Window.partitionBy("state", "generic_name").orderBy("year")
market = market.withColumn("prior_year_cost", F.lag("drug_cost").over(w))
market = market.withColumn("yoy_growth", (F.col("drug_cost") - F.col("prior_year_cost")) / F.col("prior_year_cost"))

# Cell 5: Write Gold
market.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("cms_lakehouse.gold.drug_market_share")

# Cell 6: Validate
spark.table("cms_lakehouse.gold.drug_market_share").orderBy(F.desc("drug_cost")).show(20, truncate=False)
