# Databricks notebook source
# Cell 1: Imports
import requests
from pyspark.sql import functions as F

# Cell 2: Paste your CMS UUIDs here
CMS_DATASET_IDS = {
    2018: "PASTE_2018_UUID_HERE",
    2019: "PASTE_2019_UUID_HERE",
    2020: "PASTE_2020_UUID_HERE",
    2021: "PASTE_2021_UUID_HERE",
    2022: "PASTE_2022_UUID_HERE",
}
API_BASE = "https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data"

# Cell 3: Ingestion function
def fetch_year(year: int, size: int = 5000):
    dataset_id = CMS_DATASET_IDS[year]
    if dataset_id.startswith("PASTE_"):
        raise ValueError(f"Replace CMS UUID placeholder for {year} before running ingestion.")
    offset = 0
    rows = []
    while True:
        response = requests.get(API_BASE.format(dataset_id=dataset_id), params={"size": size, "offset": offset}, timeout=60)
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        for row in batch:
            row["source_year"] = year
        rows.extend(batch)
        offset += size
    return rows

def write_bronze(year: int):
    rows = fetch_year(year)
    df = spark.createDataFrame(rows).withColumn("ingested_at", F.current_timestamp())
    (df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("cms_lakehouse.bronze.partd_raw"))
    return df.count()

# Cell 4: Run — start with 2022 only to verify before loading all years
# write_bronze(2022)

# Cell 5: Verify Bronze
spark.sql("SELECT source_year, COUNT(*) AS rows FROM cms_lakehouse.bronze.partd_raw GROUP BY source_year ORDER BY source_year").show()

# Cell 6: After 2022 verification passes, run remaining years
# for year in [2018, 2019, 2020, 2021]:
#     print(year, write_bronze(year))

# Cell 7: Final Bronze count
# spark.table("cms_lakehouse.bronze.partd_raw").count()

# Cell 8: Delta transaction log — shows all append operations
# spark.sql("DESCRIBE HISTORY cms_lakehouse.bronze.partd_raw").show(truncate=False)
