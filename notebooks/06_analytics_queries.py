# Databricks notebook source
# Query 1: Top 10 drugs by national spend (2022)
spark.sql("""
SELECT generic_name, SUM(drug_cost) AS national_spend
FROM cms_lakehouse.gold.drug_market_share
WHERE year = 2022
GROUP BY generic_name
ORDER BY national_spend DESC
LIMIT 10
""").show(truncate=False)

# Query 2: Spend per prescriber by state
spark.sql("""
SELECT state, SUM(drug_cost) / COUNT(DISTINCT provider_npi) AS spend_per_prescriber
FROM cms_lakehouse.gold.prescriber_summary
GROUP BY state
ORDER BY spend_per_prescriber DESC
""").show(truncate=False)

# Query 3: Fastest growing drugs YoY
spark.sql("""
SELECT year, state, generic_name, yoy_growth
FROM cms_lakehouse.gold.drug_market_share
WHERE yoy_growth IS NOT NULL
ORDER BY yoy_growth DESC
LIMIT 20
""").show(truncate=False)

# Query 4: Anomaly severity by specialty
spark.sql("""
SELECT specialty, COUNT(*) AS anomaly_count, AVG(z_score) AS avg_z_score, MAX(z_score) AS max_z_score
FROM cms_lakehouse.gold.spending_anomalies
GROUP BY specialty
ORDER BY anomaly_count DESC
""").show(truncate=False)

# Query 5: Delta time travel
spark.sql("DESCRIBE HISTORY cms_lakehouse.bronze.partd_raw").show(truncate=False)
