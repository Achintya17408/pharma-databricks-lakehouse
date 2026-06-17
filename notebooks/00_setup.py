# Databricks notebook source
# Cell 1: Create catalog
spark.sql("CREATE CATALOG IF NOT EXISTS cms_lakehouse")

# Cell 2: Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS cms_lakehouse.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS cms_lakehouse.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS cms_lakehouse.gold")

# Cell 3: Verify
spark.sql("SHOW SCHEMAS IN cms_lakehouse").show(truncate=False)
