# Tea vs Coffee Analytics Pipeline (Data Lake -> Warehouse -> Dashboard)

## Problem Description
This project analyzes a global dataset of tea/coffee preferences to answer questions like:
- Which `drink_category` (e.g., Modern/Traditional) is most popular?
- How do preferences and spending trends change over time (`year`)?
- How do patterns differ by `country`, `continent`, `gender`, and drink characteristics?

The goal is an end-to-end pipeline:
1. Ingest the raw dataset into a **Data Lake**.
2. Load raw data into a **Data Warehouse**.
3. Transform data (cleaning + modeling) for analytics.
4. Build a BI dashboard with at least two tiles for peer review scoring.

## Architecture (Batch Pipeline)
We implement a **batch** pipeline orchestrated with **Airflow**:
- Step A: Upload the CSV to a **GCS bucket** (Data Lake, raw zone).
- Step B: Load the CSV into **BigQuery** raw tables (Warehouse landing zone).
- Step C: Run **dbt** to transform and create curated models for analytics.
- Step D: Dashboard queries curated tables to render charts.

## Data Lake / Warehouse
- Data Lake: Google Cloud Storage (GCS) bucket
  - Path pattern: `raw/tea_vs_coffee_global_final/ingestion_date=YYYY-MM-DD/data.csv`
- Data Warehouse: BigQuery dataset
  - Raw table: `<GCP_PROJECT_ID>.<BQ_DATASET>.<BQ_RAW_TABLE>` (value of `BQ_RAW_TABLE`)
  - Curated models (dbt): `fct_tea_coffee_preferences` (dashboard-ready table)

## Warehouse Modeling (Partitioning + Clustering)
The dbt mart table used by the dashboard (model: `fct_tea_coffee_preferences`) is created as a BigQuery table with:
- `PARTITION BY` `year_date` (derived from the `year` column)
- `CLUSTER BY` `country`, `drink_category`

Rationale: most dashboard filters/aggregations group by `year` and compare results by location (`country`) and beverage type (`drink_category`), so this layout speeds up common upstream queries.

## Dashboard Requirements (2 tiles)
The Streamlit dashboard includes at least:
1. A categorical distribution chart: distribution of `drink_category`
2. A temporal chart: record counts by `year` (and optional trend of `monthly_spend` / taste)

## Terraform (Infrastructure as Code)
This repo includes Terraform templates to provision:
- a GCS bucket for the data lake
- a BigQuery dataset for the warehouse

## Reproducibility

### Prerequisites
- Python 3.10+
- Access to a GCP project
- A configured service account for GCS + BigQuery

### Install Python dependencies
```bash
pip install -r requirements.txt
```

### Configure environment variables
Copy and edit:
- `./.env.example` -> `./.env`

At minimum you need:
- `GCP_PROJECT_ID`
- `GCS_BUCKET`
- `BQ_DATASET`
- `GOOGLE_APPLICATION_CREDENTIALS` (path to service account JSON)
- `DATASET_LOCAL_PATH` (path to `tea_vs_coffee_global_final.csv`)

# Service account (recommended)
This repo's Terraform creates a dedicated service account `tea-coffee-pipeline-sa`.
For local Airflow/dbt runs, download a JSON key for it and point `GOOGLE_APPLICATION_CREDENTIALS` to that file.

### Deploy (cloud)
1. Run Terraform:
```bash
cd infra/terraform
terraform init
terraform apply
```
2. Upload dataset and run the pipeline:
   - Upload/load is automated in the Airflow DAG.
   - dbt runs inside the pipeline via `dbt run`.

### Local smoke test (optional)
If you don’t have Airflow/dbt running locally, you can still test the dashboard SQL by running:
```bash
streamlit run dashboard/app.py
```

### Extra: run common commands
```bash
make install-dev
make test
make dashboard
```

## How to run Airflow DAG (template)
In your Airflow environment:
- add `dags/tea_coffee_pipeline.py`
- ensure Airflow has DBT + Google provider credentials

Then:
```bash
airflow dags test tea_coffee_pipeline 2026-03-31
airflow dags trigger tea_coffee_pipeline
```

> Note: This repo is designed to be deployed to a real Airflow environment (Astronomer/Airflow on GCP). The DAG is production-shaped but requires your Airflow setup to be wired with the correct connections/credentials.

## Folder Overview
- `dags/`: Airflow DAG definition
- `dbt/`: dbt project (staging + marts)
- `dashboard/`: Streamlit dashboard
- `infra/terraform/`: Terraform IaC templates
- `scripts/`: helper scripts

