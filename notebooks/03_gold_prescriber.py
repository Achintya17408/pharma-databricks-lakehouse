# Databricks notebook source
# Cell 1
from pyspark.sql import functions as F
from pyspark.sql.window import Window
silver = spark.table("cms_lakehouse.silver.partd_clean")

# Cell 2: Prescriber summary with window function rank
summary = silver.groupBy("year", "provider_npi", "provider_name", "state", "specialty").agg(
    F.sum("total_claims").alias("claims"),
    F.sum("total_drug_cost").alias("drug_cost"),
    F.countDistinct("generic_name").alias("unique_drugs")
)
summary = summary.withColumn("cost_per_claim", F.col("drug_cost") / F.col("claims"))
w = Window.partitionBy("year", "state", "specialty").orderBy(F.desc("drug_cost"))
summary = summary.withColumn("state_specialty_spend_rank", F.dense_rank().over(w))

# Cell 3: Write Gold
summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("cms_lakehouse.gold.prescriber_summary")

# Cell 4: Validate
spark.table("cms_lakehouse.gold.prescriber_summary").orderBy(F.desc("drug_cost")).show(20, truncate=False)
