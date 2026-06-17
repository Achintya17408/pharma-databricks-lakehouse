# Databricks notebook source
# Cell 1: Imports
import requests
from pyspark.sql import functions as F

# Cell 2: Paste your CMS UUIDs here
CMS_DATASET_IDS = {
    2018: "802fe556-311f-4962-8d75-d5f4ff405884",
    2019: "2a6705e6-7a1e-460c-ba22-35249a531918",
    2020: "7795fe20-e80e-435a-a9ed-d2d65e05feeb",
    2021: "f68114ed-f854-4ffc-9c6e-ed78b5e2f8d0",
    2022: "b101b457-ffa4-49bb-8fd9-27c1266086e2",
}
API_BASE = "https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data"

# Cell 3: Ingestion function
def fetch_page(year: int, size: int = 5000, offset: int = 0):
    dataset_id = CMS_DATASET_IDS[year]
    if dataset_id.startswith("PASTE_"):
        raise ValueError(f"Replace CMS UUID placeholder for {year} before running ingestion.")
    response = requests.get(API_BASE.format(dataset_id=dataset_id), params={"size": size, "offset": offset}, timeout=60)
    response.raise_for_status()
    batch = response.json()
    for row in batch:
        row["source_year"] = year
    return batch

def table_exists(table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False

def write_bronze(year: int, size: int = 5000, max_pages: int | None = None):
    offset = 0
    page = 0
    total_rows = 0
    table_name = "cms_lakehouse.bronze.partd_raw"
    if table_exists(table_name):
        spark.sql(f"DELETE FROM {table_name} WHERE source_year = {year}")
    while True:
        batch = fetch_page(year, size=size, offset=offset)
        if not batch:
            break
        df = spark.createDataFrame(batch).withColumn("ingested_at", F.current_timestamp())
        (df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name))
        row_count = df.count()
        total_rows += row_count
        page += 1
        print(f"year={year} page={page} rows={row_count} total_rows={total_rows}")
        if max_pages is not None and page >= max_pages:
            break
        offset += size
    return total_rows

# Cell 4: Run — start with a small 2022 sample to verify in Databricks Free Edition
# Remove max_pages for a full-year load after the sample succeeds.
write_bronze(2022, max_pages=2)

# Cell 5: Verify Bronze
spark.sql("SELECT source_year, COUNT(*) AS rows FROM cms_lakehouse.bronze.partd_raw GROUP BY source_year ORDER BY source_year").show()

# Cell 6: After 2022 verification passes, run remaining years as small samples
# Remove max_pages for full-year loads if your Databricks workspace has enough capacity.
# for year in [2018, 2019, 2020, 2021]:
#     print(year, write_bronze(year, max_pages=2))

# Cell 7: Final Bronze count
# spark.table("cms_lakehouse.bronze.partd_raw").count()

# Cell 8: Delta transaction log — shows all append operations
# spark.sql("DESCRIBE HISTORY cms_lakehouse.bronze.partd_raw").show(truncate=False)
