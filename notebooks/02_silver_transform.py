# Databricks notebook source
# Cell 1: Imports
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

bronze = spark.table("cms_lakehouse.bronze.partd_raw")

# Cell 2: Inspect data quality
bronze.printSchema()
bronze.select("source_year").groupBy("source_year").count().orderBy("source_year").show()

# Cell 3: Transformations
silver = (
    bronze
    .withColumn("provider_npi", F.col("prscrbr_npi").cast("string"))
    .withColumn("provider_name", F.concat_ws(" ", F.col("prscrbr_first_name"), F.col("prscrbr_last_org_name")))
    .withColumn("state", F.upper(F.col("prscrbr_state_abrvtn")))
    .withColumn("specialty", F.col("prscrbr_type"))
    .withColumn("generic_name", F.upper(F.col("gnrc_name")))
    .withColumn("brand_name", F.upper(F.col("brnd_name")))
    .withColumn("total_claims", F.col("tot_clms").cast(DoubleType()))
    .withColumn("total_30day_fills", F.col("tot_30day_fills").cast(DoubleType()))
    .withColumn("total_day_supply", F.col("tot_day_suply").cast(DoubleType()))
    .withColumn("total_drug_cost", F.col("tot_drug_cst").cast(DoubleType()))
    .withColumn("year", F.col("source_year").cast(IntegerType()))
    .withColumn("cost_per_claim", F.when(F.col("total_claims") > 0, F.col("total_drug_cost") / F.col("total_claims")))
    .withColumn("transformed_at", F.current_timestamp())
)

# Cell 4: Select final columns
silver_final = silver.select(
    "year", "provider_npi", "provider_name", "state", "specialty", "generic_name", "brand_name",
    "total_claims", "total_30day_fills", "total_day_supply", "total_drug_cost", "cost_per_claim", "transformed_at"
).dropna(subset=["year", "provider_npi", "generic_name"])

# Cell 5: Write Silver partitioned by year
(silver_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("year").saveAsTable("cms_lakehouse.silver.partd_clean"))

# Cell 6: Validate
spark.table("cms_lakehouse.silver.partd_clean").groupBy("year").count().orderBy("year").show()

# Cell 7: Delta time travel demonstration
# spark.read.format("delta").option("versionAsOf", 0).table("cms_lakehouse.silver.partd_clean").show()
