# Pharma Prescriber Intelligence Lakehouse (Databricks Medallion Architecture)

> **Copilot / Agent Instruction:** All code runs inside Databricks Free Edition notebooks. Tool signup is in Sprint 1. Every story is a Databricks notebook or notebook cell. Implement in exact order. Never use pandas — use PySpark DataFrames throughout. Use the exact table names, column names, and Unity Catalog namespace specified. Export each completed notebook as a .py file for GitHub commit.

---

## Project Overview

**Business Target:** Process multi-year CMS Medicare Part D prescriber data at scale using distributed Spark computing. Surface prescriber KPIs, drug market share, and spending anomalies through a structured Gold layer queryable via Spark SQL.

**Architecture:** Medallion — Bronze (raw append-only) → Silver (clean, typed, derived) → Gold (business models)

**Dataset:** CMS Medicare Part D Prescribers by Provider and Drug — fetched via REST API inside Databricks notebooks. Stored in DBFS. No local download ever.

---

## Exact Stack

| Layer | Tool |
|---|---|
| Platform | Databricks Free Edition |
| Compute | Serverless (built into Free Edition) |
| Processing | Apache Spark 3.x + PySpark |
| Query language | Spark SQL |
| Storage format | Delta Lake |
| File system | DBFS (Databricks File System) |
| Governance | Unity Catalog (3-level namespace) |
| Language | Python (PySpark API) + SQL |

---

## Unity Catalog Namespace

```
cms_lakehouse                        ← catalog
├── bronze
│   └── partd_raw                    ← raw API data, append-only, schema inferred
├── silver
│   └── partd_clean                  ← typed, filtered, normalised, partitioned by year
└── gold
    ├── prescriber_summary           ← 1 row per NPI per year
    ├── drug_market_share            ← 1 row per drug per state per year with YoY growth
    └── spending_anomalies           ← z-score outlier prescribers
```

---

## Repository Structure

```
pharma-databricks-lakehouse/
├── notebooks/
│   ├── 00_setup.py
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_prescriber.py
│   ├── 04_gold_drug_market.py
│   ├── 05_gold_anomalies.py
│   └── 06_analytics_queries.py
├── docs/screenshots/
└── README.md
```

---

## Sprint 1 — Databricks Free Edition Setup

### Story 1.1 — Create Databricks Free Edition account
**Tasks:**
1. Go to `databricks.com/try-databricks`
2. Click "Get started" → on the next page look for "Databricks Free Edition" option (not cloud trial)
3. Sign up with email — NO credit card required
4. Verify email → log in
5. On first login: choose "Express Setup" — no AWS/GCP account needed
6. Wait for workspace provisioning (2–3 minutes)
7. You land in the Databricks workspace — note your workspace URL (format: `https://adb-XXXXXXXX.azuredatabricks.net`)

### Story 1.2 — Create GitHub repository for notebooks
**Tasks:**
1. Go to `github.com` → sign in → New repository → name: `pharma-databricks-lakehouse` → Public → Create
2. Clone locally: `git clone https://github.com/YOUR_USERNAME/pharma-databricks-lakehouse.git`
3. Create folders: `mkdir -p notebooks docs/screenshots`
4. `git add . && git commit -m "initial structure" && git push`

### Story 1.3 — Verify Spark is running in Databricks
**Tasks:**
1. In Databricks workspace: Workspace → Create → Notebook → Language: Python → name: `00_setup`
2. In first cell run:
```python
print(spark.version)
spark.range(10).show()
```
3. Click Run (▶) — if output shows Spark version and numbers 0–9, setup is complete

---

## Sprint 2 — Catalog and Schema Setup

### Story 2.1 — Notebook 00_setup: Create Unity Catalog namespace
**Notebook: `00_setup`**

```python
# Cell 1: Create catalog
spark.sql("CREATE CATALOG IF NOT EXISTS cms_lakehouse")
spark.sql("USE CATALOG cms_lakehouse")

# Cell 2: Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS cms_lakehouse.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS cms_lakehouse.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS cms_lakehouse.gold")

# Cell 3: Verify
spark.sql("SHOW SCHEMAS IN cms_lakehouse").show()
```

**Expected output:** Three schemas listed: bronze, silver, gold

**Export and commit:**
1. File → Export → Source File (.py) → save to `notebooks/00_setup.py`
2. `git add notebooks/00_setup.py && git commit -m "feat: catalog and schema setup" && git push`

---

## Sprint 3 — CMS API Discovery

### Story 3.1 — Find CMS dataset UUIDs manually
**Tasks (do this in a browser, not Databricks):**
1. Go to `data.cms.gov`
2. Search: "Medicare Part D Prescribers by Provider and Drug"
3. Click the dataset page → for each year (2020, 2021, 2022, 2023): click that year → click "API" tab
4. Copy the UUID from the API endpoint URL. Format: `9552919d-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
5. Keep these 4 UUIDs ready for Sprint 4 — paste them into the notebook as instructed

---

## Sprint 4 — Bronze Layer: Raw Ingest

### Story 4.1 — Notebook 01_bronze_ingest: Stream API → Bronze Delta table

**Design principle:** Bronze is append-only. Raw data lands here exactly as received. No transformations. Schema inferred automatically. Each year appends to the same table.

**Notebook: `01_bronze_ingest`**

```python
# Cell 1: Imports
import requests
from pyspark.sql import SparkSession

spark.sql("USE CATALOG cms_lakehouse")

# Cell 2: Paste your CMS UUIDs here
YEAR_UUIDS = {
    2020: "PASTE-2020-UUID-HERE",
    2021: "PASTE-2021-UUID-HERE",
    2022: "PASTE-2022-UUID-HERE",
    2023: "PASTE-2023-UUID-HERE",
}
CMS_BASE   = "https://data.cms.gov/data-api/v1/dataset/{uuid}/data"
BATCH_SIZE = 5000

# Cell 3: Ingestion function
def ingest_year(year: int, uuid: str):
    url    = CMS_BASE.format(uuid=uuid)
    offset = 0
    total  = 0

    while True:
        resp  = requests.get(url, params={"size": BATCH_SIZE, "offset": offset})
        resp.raise_for_status()
        batch = resp.json()

        if not batch:
            print(f"Year {year} complete — {total:,} rows loaded")
            break

        for row in batch:
            row["data_year"] = str(year)

        batch_df = spark.createDataFrame(batch)
        batch_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("cms_lakehouse.bronze.partd_raw")

        total  += len(batch)
        offset += BATCH_SIZE
        if offset % 50000 == 0:
            print(f"  Year {year}: {total:,} rows so far...")

# Cell 4: Run — start with 2022 only to verify before loading all years
print("=== Ingesting 2022 (verification run) ===")
ingest_year(2022, YEAR_UUIDS[2022])

# Cell 5: Verify Bronze
spark.sql("SELECT COUNT(*) as total_rows FROM cms_lakehouse.bronze.partd_raw").show()
spark.sql("SELECT data_year, COUNT(*) FROM cms_lakehouse.bronze.partd_raw GROUP BY data_year").show()

# Cell 6: After 2022 verification passes, run remaining years
for year in [2020, 2021, 2023]:
    print(f"\n=== Ingesting {year} ===")
    ingest_year(year, YEAR_UUIDS[year])

# Cell 7: Final Bronze count
spark.sql("""
    SELECT data_year, COUNT(*) as rows, COUNT(DISTINCT prscrbr_npi) as unique_providers
    FROM cms_lakehouse.bronze.partd_raw
    GROUP BY data_year ORDER BY data_year
""").show()

# Cell 8: Delta transaction log — shows all append operations
spark.sql("DESCRIBE HISTORY cms_lakehouse.bronze.partd_raw").show(10, truncate=False)
```

**Export and commit:**
1. File → Export → Source File → save to `notebooks/01_bronze_ingest.py`
2. `git add notebooks/ && git commit -m "feat: bronze ingestion layer" && git push`

---

## Sprint 5 — Silver Layer: Clean, Type, Derive

### Story 5.1 — Notebook 02_silver_transform

**Design principle:** Silver is idempotent (safe to re-run). Overwrites on each run. Partitioned by year for query performance.

**Notebook: `02_silver_transform`**

```python
# Cell 1: Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE CATALOG cms_lakehouse")
bronze = spark.table("cms_lakehouse.bronze.partd_raw")
print(f"Bronze row count: {bronze.count():,}")
bronze.printSchema()

# Cell 2: Inspect data quality
print("=== NULL COUNTS ===")
bronze.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in bronze.columns]).show()

# Cell 3: Transformations
silver = bronze \
    .filter(F.col("prscrbr_npi").isNotNull()) \
    .filter(F.col("gnrc_name").isNotNull()) \
    .filter(F.col("tot_clms").isNotNull()) \
    .filter(F.col("tot_drug_cst").isNotNull()) \
    .withColumn("npi",
        F.col("prscrbr_npi").cast("string")) \
    .withColumn("total_claims",
        F.regexp_replace(F.col("tot_clms").cast("string"), "[^0-9.]", "").cast("integer")) \
    .withColumn("total_drug_cost",
        F.regexp_replace(F.col("tot_drug_cst").cast("string"), "[^0-9.]", "").cast("double")) \
    .withColumn("year",
        F.col("data_year").cast("integer")) \
    .withColumn("specialty",   F.upper(F.trim(F.col("prscrbr_type")))) \
    .withColumn("state",       F.upper(F.trim(F.col("prscrbr_state_abrvtn")))) \
    .withColumn("generic_name",F.upper(F.trim(F.col("gnrc_name")))) \
    .withColumn("brand_name",  F.upper(F.trim(F.col("brnd_name")))) \
    .withColumn("city",        F.initcap(F.trim(F.col("prscrbr_city")))) \
    .withColumn("last_name",   F.initcap(F.trim(F.col("prscrbr_last_org_name")))) \
    .withColumn("first_name",  F.initcap(F.trim(F.col("prscrbr_first_name")))) \
    .withColumn("cost_per_claim",
        F.when(F.col("total_claims") > 0,
               F.round(F.col("total_drug_cost") / F.col("total_claims"), 2))
         .otherwise(None)) \
    .filter(F.col("total_claims") > 0) \
    .filter(F.col("total_drug_cost") > 0) \
    .dropDuplicates(["npi", "generic_name", "year"])

# Cell 4: Select final columns
silver_final = silver.select(
    "npi","last_name","first_name","city","state","specialty",
    "generic_name","brand_name","total_claims","total_drug_cost",
    "cost_per_claim","year"
)
print(f"Silver row count: {silver_final.count():,}")

# Cell 5: Write Silver partitioned by year
silver_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year") \
    .saveAsTable("cms_lakehouse.silver.partd_clean")

# Cell 6: Validate
spark.sql("""
    SELECT year, COUNT(*) as rows, ROUND(SUM(total_drug_cost)/1e9,2) as spend_billions
    FROM cms_lakehouse.silver.partd_clean
    GROUP BY year ORDER BY year
""").show()

# Cell 7: Delta time travel demonstration
spark.sql("DESCRIBE HISTORY cms_lakehouse.silver.partd_clean").show(5, truncate=False)
# To query a previous version: spark.read.format("delta").option("versionAsOf", 0).table("cms_lakehouse.silver.partd_clean")
```

**Export:** File → Export → Source File → `notebooks/02_silver_transform.py` → commit

---

## Sprint 6 — Gold Layer: Prescriber Summary

### Story 6.1 — Notebook 03_gold_prescriber

**Notebook: `03_gold_prescriber`**

```python
# Cell 1
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE CATALOG cms_lakehouse")
silver = spark.table("cms_lakehouse.silver.partd_clean")

# Cell 2: Prescriber summary with window function rank
window_spec = Window.partitionBy("specialty","state","year").orderBy(F.desc("total_drug_cost"))

prescriber_summary = silver.groupBy(
    "npi","last_name","first_name","city","state","specialty","year"
).agg(
    F.sum("total_claims").alias("total_claims"),
    F.round(F.sum("total_drug_cost"),2).alias("total_drug_cost"),
    F.round(F.avg("cost_per_claim"),2).alias("avg_cost_per_claim"),
    F.countDistinct("generic_name").alias("unique_drugs_prescribed"),
    F.round(F.sum("total_drug_cost")/F.sum("total_claims"),2).alias("overall_cost_per_claim")
).withColumn("rank_in_specialty_state", F.rank().over(window_spec))

# Cell 3: Write Gold
prescriber_summary.write \
    .format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("cms_lakehouse.gold.prescriber_summary")

# Cell 4: Validate
spark.sql("""
    SELECT state, specialty, year, COUNT(*) as prescribers, ROUND(SUM(total_drug_cost)/1e6,1) as spend_M
    FROM cms_lakehouse.gold.prescriber_summary
    GROUP BY state, specialty, year ORDER BY spend_M DESC LIMIT 10
""").show()
```

**Export:** `notebooks/03_gold_prescriber.py` → commit

---

## Sprint 7 — Gold Layer: Drug Market Share

### Story 7.1 — Notebook 04_gold_drug_market

**Notebook: `04_gold_drug_market`**

```python
# Cell 1
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE CATALOG cms_lakehouse")
silver = spark.table("cms_lakehouse.silver.partd_clean")

# Cell 2: State totals for market share denominator
state_total = silver.groupBy("state","year") \
    .agg(F.sum("total_drug_cost").alias("state_total_spend"))

# Cell 3: Drug-state-year aggregation
drug_state = silver.groupBy("generic_name","brand_name","state","year").agg(
    F.sum("total_claims").alias("total_claims"),
    F.round(F.sum("total_drug_cost"),2).alias("total_drug_cost"),
    F.countDistinct("npi").alias("unique_prescribers")
)

# Cell 4: Add market share + YoY growth
yoy_w = Window.partitionBy("generic_name","state").orderBy("year")
drug_market = drug_state.join(state_total, on=["state","year"], how="left") \
    .withColumn("market_share_pct",
        F.round(F.col("total_drug_cost")/F.col("state_total_spend")*100, 4)) \
    .withColumn("prev_year_cost", F.lag("total_drug_cost").over(yoy_w)) \
    .withColumn("yoy_growth_pct",
        F.when(F.col("prev_year_cost").isNotNull() & (F.col("prev_year_cost") > 0),
               F.round((F.col("total_drug_cost")-F.col("prev_year_cost"))/F.col("prev_year_cost")*100,2))
         .otherwise(None))

# Cell 5: Write Gold
drug_market.write \
    .format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("cms_lakehouse.gold.drug_market_share")

# Cell 6: Validate
spark.sql("""
    SELECT generic_name, state, year, total_drug_cost, market_share_pct, yoy_growth_pct
    FROM cms_lakehouse.gold.drug_market_share
    WHERE state = 'CA' AND year = 2022
    ORDER BY total_drug_cost DESC LIMIT 10
""").show(truncate=False)
```

**Export:** `notebooks/04_gold_drug_market.py` → commit

---

## Sprint 8 — Gold Layer: Spending Anomalies

### Story 8.1 — Notebook 05_gold_anomalies

**Notebook: `05_gold_anomalies`**

```python
# Cell 1
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE CATALOG cms_lakehouse")
silver = spark.table("cms_lakehouse.silver.partd_clean")

# Cell 2: Compute specialty-state-year benchmarks
bw = Window.partitionBy("specialty","state","year")
silver_b = silver \
    .withColumn("spec_state_median", F.percentile_approx("cost_per_claim",0.5).over(bw)) \
    .withColumn("spec_state_mean",   F.avg("cost_per_claim").over(bw)) \
    .withColumn("spec_state_std",    F.stddev("cost_per_claim").over(bw))

# Cell 3: Z-score and flag anomalies (threshold: z > 2.0)
anomalies = silver_b \
    .withColumn("z_score",
        F.when(F.col("spec_state_std") > 0,
               (F.col("cost_per_claim") - F.col("spec_state_mean")) / F.col("spec_state_std"))
         .otherwise(None)) \
    .filter(F.col("z_score") > 2.0) \
    .select("npi","last_name","first_name","city","state","specialty",
            "generic_name","total_claims","total_drug_cost","cost_per_claim",
            "spec_state_median","spec_state_mean","z_score","year") \
    .orderBy(F.desc("z_score"))

# Cell 4: Write Gold
anomalies.write \
    .format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("cms_lakehouse.gold.spending_anomalies")

print(f"Anomalous prescribers: {anomalies.count():,}")
spark.sql("""
    SELECT specialty, state, year, COUNT(*) as anomaly_count, ROUND(AVG(z_score),2) as avg_z
    FROM cms_lakehouse.gold.spending_anomalies
    GROUP BY specialty, state, year ORDER BY anomaly_count DESC LIMIT 15
""").show()
```

**Export:** `notebooks/05_gold_anomalies.py` → commit

---

## Sprint 9 — Analytics Queries and Validation

### Story 9.1 — Notebook 06_analytics_queries

**Notebook: `06_analytics_queries`**

```python
spark.sql("USE CATALOG cms_lakehouse")

# Query 1: Top 10 drugs by national spend (2022)
spark.sql("""
    SELECT generic_name, SUM(total_drug_cost) as national_spend, SUM(total_claims) as claims
    FROM cms_lakehouse.gold.drug_market_share
    WHERE year = 2022 GROUP BY generic_name ORDER BY national_spend DESC LIMIT 10
""").show(truncate=False)

# Query 2: Spend per prescriber by state
spark.sql("""
    SELECT state, year,
           ROUND(SUM(total_drug_cost)/1e6,1) as spend_M,
           COUNT(DISTINCT npi) as prescribers,
           ROUND(SUM(total_drug_cost)/COUNT(DISTINCT npi),0) as spend_per_prescriber
    FROM cms_lakehouse.gold.prescriber_summary
    GROUP BY state, year ORDER BY spend_per_prescriber DESC LIMIT 15
""").show()

# Query 3: Fastest growing drugs YoY
spark.sql("""
    SELECT generic_name, AVG(yoy_growth_pct) as avg_yoy_growth,
           SUM(total_drug_cost) as total_spend
    FROM cms_lakehouse.gold.drug_market_share
    WHERE yoy_growth_pct IS NOT NULL GROUP BY generic_name
    HAVING COUNT(*) >= 10 ORDER BY avg_yoy_growth DESC LIMIT 10
""").show(truncate=False)

# Query 4: Anomaly severity by specialty
spark.sql("""
    SELECT specialty, COUNT(*) as anomalous_prescribers,
           ROUND(AVG(z_score),2) as avg_z_score,
           ROUND(AVG(cost_per_claim),2) as avg_cpc,
           ROUND(AVG(spec_state_median),2) as specialty_median_cpc
    FROM cms_lakehouse.gold.spending_anomalies WHERE year = 2022
    GROUP BY specialty ORDER BY anomalous_prescribers DESC LIMIT 15
""").show(truncate=False)

# Query 5: Delta time travel
spark.sql("DESCRIBE HISTORY cms_lakehouse.silver.partd_clean").show(5, truncate=False)
```

**Export:** `notebooks/06_analytics_queries.py` → commit

---

## Sprint 10 — Final Push and Screenshots

### Story 10.1 — Take required screenshots
**Tasks:**
1. In Databricks: go to Catalog Explorer → cms_lakehouse → take screenshot showing bronze/silver/gold schemas → save to `docs/screenshots/unity_catalog.png`
2. Run `DESCRIBE HISTORY cms_lakehouse.bronze.partd_raw` → screenshot showing multiple appends → `docs/screenshots/bronze_history.png`
3. Run Query 1 (top drugs) → screenshot result → `docs/screenshots/gold_top_drugs.png`
4. `git add docs/ && git commit -m "docs: add databricks screenshots" && git push`

### Story 10.2 — Final commit all notebooks
**Tasks:**
1. `git add notebooks/ && git commit -m "feat: complete medallion lakehouse — all 6 notebooks" && git push`
2. Verify on GitHub: all 6 .py files visible in notebooks/

---

## Definition of Done
- [ ] `SHOW SCHEMAS IN cms_lakehouse` returns bronze, silver, gold
- [ ] Bronze row count > 1M across all years
- [ ] `DESCRIBE HISTORY bronze.partd_raw` shows multiple append transactions
- [ ] Silver row count < Bronze (nulls and dupes removed)
- [ ] Silver `DESCRIBE DETAIL` shows partitionBy year
- [ ] All 3 Gold tables queryable — row counts > 0
- [ ] Anomalies table uses z_score > 2.0 threshold
- [ ] Window functions used in Gold layers (not subqueries)
- [ ] All 6 notebooks exported as .py and committed to GitHub
- [ ] Screenshots in docs/screenshots/
