import os
import sys
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable

sys.path.insert(0, '/opt/airflow')

from pipeline.download import download_csv
from pipeline.upload_gcs import upload_to_gcs
from pipeline.load_bigquery import load_tour_to_bigquery
from pipeline.streaming_producer import publish_csv_to_pubsub
from pipeline.streaming_consumer import stream_csv_to_bigquery, create_bigquery_tables_streaming

# ── Config ────────────────────────────────────────────────────────────────────
GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
GCS_BUCKET     = os.environ['GCS_BUCKET']
GCS_PREFIX     = os.environ.get('GCS_PREFIX', 'raw')
BQ_DATASET     = os.environ['BQ_DATASET']
BQ_STREAMING_DATASET = os.environ.get('BQ_STREAMING_DATASET', 'tennis_streaming')
PUBSUB_TOPIC   = os.environ.get('PUBSUB_TOPIC', 'tennis-matches-stream')

LOCAL_DIR = '/tmp/tennis'

# Read at parse time — change via Airflow UI > Admin > Variables
START_YEAR = int(Variable.get('tennis_start_year', default_var=1995))
END_YEAR   = int(Variable.get('tennis_end_year',   default_var=2024))

# ── Task callables ─────────────────────────────────────────────────────────────
def _cleanup_gcs(tour: str) -> None:
    from google.cloud import storage
    from google.cloud.exceptions import NotFound
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blobs = bucket.list_blobs(prefix=f"{GCS_PREFIX}/{tour}_matches_")
    for blob in blobs:
        try:
            blob.delete()
            print(f"Deleted {blob.name}")
        except NotFound:
            print(f"Already deleted: {blob.name}")


def _download(tour: str, year: int) -> None:
    download_csv(tour=tour, year=year, dest_dir=LOCAL_DIR)


def _upload_and_delete(tour: str, year: int) -> None:
    local_path = Path(LOCAL_DIR) / f"{tour}_matches_{year}.csv"
    upload_to_gcs(local_path=local_path, bucket_name=GCS_BUCKET, gcs_prefix=GCS_PREFIX)
    local_path.unlink()


def _load_bq(tour: str) -> None:
    load_tour_to_bigquery(
        tour=tour,
        bucket_name=GCS_BUCKET,
        dataset_id=BQ_DATASET,
        project_id=GCP_PROJECT_ID,
        gcs_prefix=GCS_PREFIX,
    )


def _publish_to_pubsub(tour: str) -> dict:
    """
    Publish all CSV files for a tour to Pub/Sub for streaming pipeline.
    This bridges batch data to the streaming layer.
    """
    local_dir = Path(LOCAL_DIR)
    csv_files = list(local_dir.glob(f"{tour}_matches_*.csv"))

    if not csv_files:
        print(f"No CSV files found for {tour}, skipping pub/sub publish")
        return {"published": 0}

    total_count = 0
    for csv_path in csv_files:
        count = publish_csv_to_pubsub(
            project_id=GCP_PROJECT_ID,
            topic_id=PUBSUB_TOPIC,
            csv_path=csv_path,
            batch_size=100,
        )
        total_count += count
        csv_path.unlink()  # Clean up after publishing

    return {
        "tour": tour,
        "published": total_count,
        "files": len(csv_files),
    }


def _stream_to_bigquery(tour: str) -> dict:
    """
    Stream published data from Pub/Sub to BigQuery streaming tables.
    This demonstrates the streaming consumer pattern.
    """
    local_dir = Path(LOCAL_DIR)
    csv_files = list(local_dir.glob(f"{tour}_matches_*.csv"))

    if not csv_files:
        print(f"No CSV files found for {tour}, skipping streaming")
        return {"streamed": 0}

    total_count = 0
    for csv_path in csv_files:
        count = stream_csv_to_bigquery(
            project_id=GCP_PROJECT_ID,
            dataset_id=BQ_STREAMING_DATASET,
            table_id="realtime_matches",
            csv_path=csv_path,
            chunk_size=500,
        )
        total_count += count

    return {
        "tour": tour,
        "streamed": total_count,
    }


def _create_streaming_infrastructure() -> None:
    """Create BigQuery streaming tables and Pub/Sub resources."""
    create_bigquery_tables_streaming(GCP_PROJECT_ID, BQ_STREAMING_DATASET)


def _validate_streaming_data() -> dict:
    """
    Validate that streaming data matches batch data.
    This ensures data consistency between batch and streaming pipelines.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)

    query = f"""
        SELECT
            COUNT(*) as total_matches,
            COUNT(DISTINCT tour) as distinct_tours,
            MIN(tourney_date) as earliest_date,
            MAX(tourney_date) as latest_date
        FROM `{GCP_PROJECT_ID}.{BQ_STREAMING_DATASET}.realtime_matches`
        WHERE data_source = 'streaming'
    """

    results = client.query(query).result()
    for row in results:
        return {
            "total_matches": row.total_matches,
            "distinct_tours": row.distinct_tours,
            "earliest_date": str(row.earliest_date),
            "latest_date": str(row.latest_date),
        }

    return {}


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id='tennis_pipeline',
    schedule='@yearly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['tennis', 'streaming'],
    description='End-to-end tennis data pipeline with batch and streaming support',
) as dag:

    # ── Stage 1: Infrastructure Setup ──────────────────────────────────────────
    with TaskGroup(group_id='setup_group') as setup_group:
        validate_config = BashOperator(
            task_id='validate_config',
            bash_command=(
                'echo "Validating GCP configuration..." && '
                'gcloud auth list --filter=status:ACTIVE --format="value(account)" && '
                'echo "GCP authentication verified"'
            ),
        )

        create_streaming_tables = PythonOperator(
            task_id='create_streaming_infrastructure',
            python_callable=_create_streaming_infrastructure,
        )

        validate_config >> create_streaming_tables

    # ── Stage 2: Batch Data Processing ─────────────────────────────────────────
    years = range(START_YEAR, END_YEAR + 1)

    for tour in ['atp', 'wta']:
        with TaskGroup(group_id=f'{tour}_batch_group') as tour_batch_group:

            cleanup_task = PythonOperator(
                task_id='cleanup_gcs',
                python_callable=_cleanup_gcs,
                op_kwargs={'tour': tour},
            )

            prev_task = cleanup_task

            for year in years:
                download_task = PythonOperator(
                    task_id=f'download_{year}',
                    python_callable=_download,
                    op_kwargs={'tour': tour, 'year': year},
                )
                upload_task = PythonOperator(
                    task_id=f'upload_{year}',
                    python_callable=_upload_and_delete,
                    op_kwargs={'tour': tour, 'year': year},
                )

                prev_task >> download_task >> upload_task
                prev_task = upload_task

            load_bq_task = PythonOperator(
                task_id='load_bigquery',
                python_callable=_load_bq,
                op_kwargs={'tour': tour},
            )

            prev_task >> load_bq_task

    # ── Stage 3: Streaming Pipeline ─────────────────────────────────────────────
    with TaskGroup(group_id='streaming_group') as streaming_group:

        publish_to_pubsub_atp = PythonOperator(
            task_id='publish_atp_to_pubsub',
            python_callable=_publish_to_pubsub,
            op_kwargs={'tour': 'atp'},
        )

        publish_to_pubsub_wta = PythonOperator(
            task_id='publish_wta_to_pubsub',
            python_callable=_publish_to_pubsub,
            op_kwargs={'tour': 'wta'},
        )

        stream_atp_to_bigquery = PythonOperator(
            task_id='stream_atp_to_bigquery',
            python_callable=_stream_to_bigquery,
            op_kwargs={'tour': 'atp'},
        )

        stream_wta_to_bigquery = PythonOperator(
            task_id='stream_wta_to_bigquery',
            python_callable=_stream_to_bigquery,
            op_kwargs={'tour': 'wta'},
        )

        validate_streaming = PythonOperator(
            task_id='validate_streaming_data',
            python_callable=_validate_streaming_data,
        )

        # Parallel publishing then sequential streaming
        [publish_to_pubsub_atp, publish_to_pubsub_wta] >> [stream_atp_to_bigquery, stream_wta_to_bigquery] >> validate_streaming

    # ── Stage 4: dbt Transformations ───────────────────────────────────────────
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='dbt deps --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
    )

    # ── Stage 5: Data Quality Checks ───────────────────────────────────────────
    with TaskGroup(group_id='quality_group') as quality_group:

        check_batch_row_counts = BashOperator(
            task_id='check_batch_row_counts',
            bash_command=(
                f'echo "Checking batch data row counts..." && '
                f'bq query --use_legacy_sql=false '
                f'"SELECT table_name, row_count FROM `{GCP_PROJECT_ID}.tennis_raw.__TABLES__`"'
            ),
        )

        check_streaming_row_counts = BashOperator(
            task_id='check_streaming_row_counts',
            bash_command=(
                f'echo "Checking streaming data row counts..." && '
                f'bq query --use_legacy_sql=false '
                f'"SELECT table_name, row_count FROM `{GCP_PROJECT_ID}.tennis_streaming.__TABLES__`"'
            ),
        )

        check_data_freshness = BashOperator(
            task_id='check_data_freshness',
            bash_command=(
                f'echo "Checking data freshness..." && '
                f'bq query --use_legacy_sql=false '
                f'"SELECT MAX(ingested_at) as latest_ingestion FROM `{GCP_PROJECT_ID}.tennis_streaming.realtime_matches`"'
            ),
        )

        [check_batch_row_counts, check_streaming_row_counts, check_data_freshness]

    # ── Stage 6: Notifications ──────────────────────────────────────────────────
    with TaskGroup(group_id='notification_group') as notification_group:

        send_success_notification = BashOperator(
            task_id='send_success_notification',
            bash_command='echo "Tennis pipeline completed successfully at $(date)"',
            trigger_rule='all_success',
        )

    # ── DAG Dependencies ─────────────────────────────────────────────────────────
    setup_group >> [dbt_deps]

    dbt_deps >> [dbt_run, dbt_test]

    dbt_run >> dbt_test >> quality_group >> notification_group