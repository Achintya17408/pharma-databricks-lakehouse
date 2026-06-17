# Pharma Prescriber Intelligence Lakehouse

Databricks Free Edition medallion architecture for CMS Medicare Part D data using Bronze, Silver, and Gold Delta tables under Unity Catalog namespace `cms_lakehouse`.

The original assignment is preserved exactly in `PROJECT_SPEC.md`.

## Notebook Order

1. `notebooks/00_setup.py`
2. `notebooks/01_bronze_ingest.py`
3. `notebooks/02_silver_transform.py`
4. `notebooks/03_gold_prescriber.py`
5. `notebooks/04_gold_drug_market.py`
6. `notebooks/05_gold_anomalies.py`
7. `notebooks/06_analytics_queries.py`
