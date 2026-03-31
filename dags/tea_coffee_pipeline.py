from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from google.cloud import storage
from google.cloud import bigquery


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def upload_csv_to_gcs(ds: str, **_context) -> str:
    """Upload local CSV into a GCS raw zone (data lake)."""
    dataset_local_path = Path(_require_env("DATASET_LOCAL_PATH"))
    if not dataset_local_path.exists():
        raise FileNotFoundError(
            f"DATASET_LOCAL_PATH does not exist: {dataset_local_path}. "
            "Make sure the Airflow worker environment can access the file."
        )

    gcs_bucket = _require_env("GCS_BUCKET")
    object_path = (
        f"raw/tea_vs_coffee_global_final/ingestion_date={ds}/"
        f"tea_vs_coffee_global_final.csv"
    )
    gcs_uri = f"gs://{gcs_bucket}/{object_path}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(object_path)

    blob.upload_from_filename(str(dataset_local_path))
    return gcs_uri


def load_gcs_csv_to_bigquery(ds: str, ti, **_context) -> None:
    """Load the raw CSV from GCS into a BigQuery raw table."""
    gcp_project_id = _require_env("GCP_PROJECT_ID")
    bq_dataset = _require_env("BQ_DATASET")
    bq_raw_table = _require_env("BQ_RAW_TABLE")

    # The previous task returns the source URI.
    gcs_uri = ti.xcom_pull(task_ids="upload_to_gcs")
    if not gcs_uri:
        raise RuntimeError("Missing XCom source URI from upload_to_gcs task.")

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
    load_job.result()  # Wait synchronously for completion.


def _repo_root() -> Path:
    # dags/tea_coffee_pipeline.py -> repo root
    return Path(__file__).resolve().parents[1]


default_args = {
    "owner": os.getenv("PIPELINE_OWNER", "student"),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

_gcp_keyfile = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if _gcp_keyfile:
    # Ensure google clients (storage/bigquery) and dbt share the same credentials.
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _gcp_keyfile


with DAG(
    dag_id="tea_coffee_pipeline",
    default_args=default_args,
    description="Upload tea/coffee dataset to GCS, load to BigQuery, run dbt transformations, ready dashboard tables.",
    schedule_interval="0 2 * * *",  # daily batch
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    start = EmptyOperator(task_id="start")

    upload = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_csv_to_gcs,
        op_kwargs={"ds": "{{ ds }}"},
    )

    load_raw = PythonOperator(
        task_id="load_raw_to_bigquery",
        python_callable=load_gcs_csv_to_bigquery,
        op_kwargs={"ds": "{{ ds }}"},
    )

    repo_root = _repo_root()
    dbt_cmd = (
        f"cd \"{repo_root}\" && "
        "dbt run "
        "--project-dir dbt "
        "--profiles-dir dbt "
        "--target prod"
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=dbt_cmd,
        env={
            # Ensure dbt BigQuery profile can find required creds.
            "GOOGLE_APPLICATION_CREDENTIALS": os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""),
        },
    )

    end = EmptyOperator(task_id="end")

    start >> upload >> load_raw >> run_dbt >> end

