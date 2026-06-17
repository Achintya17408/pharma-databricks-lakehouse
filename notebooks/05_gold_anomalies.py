# Databricks notebook source
# Cell 1
from pyspark.sql import functions as F
silver = spark.table("cms_lakehouse.silver.partd_clean")

# Cell 2: Compute specialty-state-year benchmarks
provider = silver.groupBy("year", "state", "specialty", "provider_npi", "provider_name").agg(
    F.sum("total_claims").alias("claims"),
    F.sum("total_drug_cost").alias("drug_cost")
).withColumn("cost_per_claim", F.col("drug_cost") / F.col("claims"))
bench = provider.groupBy("year", "state", "specialty").agg(
    F.avg("cost_per_claim").alias("avg_cost_per_claim"),
    F.stddev("cost_per_claim").alias("std_cost_per_claim")
)

# Cell 3: Z-score and flag anomalies (threshold: z > 2.0)
anomalies = provider.join(bench, ["year", "state", "specialty"], "left")
anomalies = anomalies.withColumn(
    "z_score",
    F.when(F.col("std_cost_per_claim") > 0, (F.col("cost_per_claim") - F.col("avg_cost_per_claim")) / F.col("std_cost_per_claim"))
)
anomalies = anomalies.withColumn("is_anomaly", F.col("z_score") > 2.0)
anomalies = anomalies.filter(F.col("is_anomaly") == True)

# Cell 4: Write Gold
anomalies.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("cms_lakehouse.gold.spending_anomalies")
spark.table("cms_lakehouse.gold.spending_anomalies").orderBy(F.desc("z_score")).show(50, truncate=False)
