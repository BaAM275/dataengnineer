from __future__ import annotations

import argparse
import os
from datetime import date

from google.cloud import bigquery


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion-date", default=str(date.today()), help="YYYY-MM-DD")
    args = parser.parse_args()

    gcp_project_id = _require_env("GCP_PROJECT_ID")
    gcs_bucket = _require_env("GCS_BUCKET")
    bq_dataset = _require_env("BQ_DATASET")
    bq_raw_table = _require_env("BQ_RAW_TABLE")

    gcs_uri = (
        f"gs://{gcs_bucket}/raw/tea_vs_coffee_global_final/ingestion_date={args.ingestion_date}/"
        "tea_vs_coffee_global_final.csv"
    )

    client = bigquery.Client(project=gcp_project_id)
    destination = f"{gcp_project_id}.{bq_dataset}.{bq_raw_table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        destination,
        job_config=job_config,
    )
    load_job.result()
    print(f"Loaded into {destination}")


if __name__ == "__main__":
    main()

