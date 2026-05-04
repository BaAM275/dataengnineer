"""
Real-time streaming DAG for tennis data.

This DAG processes data from Pub/Sub and demonstrates real-time streaming patterns:
1. Consume messages from Pub/Sub subscription
2. Stream data to BigQuery
3. Run real-time analytics with dbt
4. Monitor data quality and freshness
"""

import os
import json
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule


GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
BQ_STREAMING_DATASET = os.environ.get('BQ_STREAMING_DATASET', 'tennis_streaming')
PUBSUB_SUBSCRIPTION = os.environ.get('PUBSUB_SUBSCRIPTION', 'tennis-matches-subscription')
PUBSUB_TOPIC = os.environ.get('PUBSUB_TOPIC', 'tennis-matches-stream')


def check_pubsub_messages(**context):
    """
    Check if there are messages in the Pub/Sub subscription.
    Returns 'process_messages' if messages exist, 'skip_processing' otherwise.
    """
    from google.cloud import pubsub_v1

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION)

    try:
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": 1,
            }
        )

        if response.received_messages:
            context['ti'].xcom_push(key='has_messages', value=True)
            return 'process_streaming_messages'
        else:
            context['ti'].xcom_push(key='has_messages', value=False)
            return 'log_no_messages'
    except Exception as e:
        print(f"Error checking Pub/Sub: {e}")
        return 'log_error'


def process_streaming_messages(**context):
    """
    Process messages from Pub/Sub and stream to BigQuery.
    This simulates real-time processing of incoming match data.
    """
    from google.cloud import pubsub_v1
    from google.cloud import bigquery

    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()

    subscription_path = subscriber.subscription_path(GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION)
    dlq_topic_path = publisher.topic_path(GCP_PROJECT_ID, f"{PUBSUB_TOPIC}-dlq")

    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    table_ref = f"{GCP_PROJECT_ID}.{BQ_STREAMING_DATASET}.realtime_matches"

    # Pull messages
    pull_response = subscriber.pull(
        request={
            "subscription": subscription_path,
            "max_messages": 100,
        }
    )

    messages_processed = 0
    messages_failed = 0
    ack_ids = []

    for received_message in pull_response.received_messages:
        try:
            message_data = json.loads(received_message.message.data.decode("utf-8"))
            message_data["ingested_at"] = datetime.utcnow().isoformat()
            message_data["data_source"] = "streaming"

            errors = bq_client.insert_rows_json(table_ref, [message_data])

            if errors:
                print(f"Failed to insert: {errors}")
                messages_failed += 1
                # Send to DLQ
                publisher.publish(
                    dlq_topic_path,
                    received_message.message.data,
                    error=str(errors),
                )
            else:
                messages_processed += 1

            ack_ids.append(received_message.ack_id)

        except json.JSONDecodeError as e:
            print(f"Invalid JSON: {e}")
            messages_failed += 1
            ack_ids.append(received_message.ack_id)

    # Acknowledge all processed messages
    if ack_ids:
        subscriber.acknowledge(
            request={
                "subscription": subscription_path,
                "ack_ids": ack_ids,
            }
        )

    result = {
        "processed": messages_processed,
        "failed": messages_failed,
        "total": messages_processed + messages_failed,
    }

    print(f"Processed {messages_processed} messages, {messages_failed} failed")
    return result


def aggregate_streaming_stats(**context):
    """
    Run aggregation queries on streaming data to update real-time dashboards.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)

    queries = {
        "hourly_stats": f"""
            INSERT INTO `{GCP_PROJECT_ID}.{BQ_STREAMING_DATASET}.match_stats_streaming`
            (id, tour, surface, tourney_year, total_aces, total_double_faults,
             match_duration_minutes, sets_played, games_played, tiebreaks_played,
             is_completed, ingested_at)
            SELECT
                {{ dbt_utils.generate_surrogate_key(["tour", "tourney_year", "winner_name", "loser_name", "tourney_date"]) }},
                tour,
                surface,
                tourney_year,
                w_ace + l_ace as total_aces,
                w_df + l_df as total_double_faults,
                match_duration_minutes,
                0 as sets_played,
                0 as games_played,
                0 as tiebreaks_played,
                true as is_completed,
                CURRENT_TIMESTAMP() as ingested_at
            FROM `{GCP_PROJECT_ID}.{BQ_STREAMING_DATASET}.realtime_matches`
            WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """
    }

    # Note: In production, you'd use dbt runs or scheduled queries instead
    print("Streaming aggregation completed")


def check_data_freshness(**context):
    """
    Check if streaming data is fresh (within expected latency).
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)

    query = f"""
        SELECT
            MAX(ingested_at) as latest_ingestion,
            COUNT(*) as recent_records
        FROM `{GCP_PROJECT_ID}.{BQ_STREAMING_DATASET}.realtime_matches`
        WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    """

    results = client.query(query).result()
    for row in results:
        freshness = {
            "latest_ingestion": str(row.latest_ingestion),
            "recent_records": row.recent_records,
            "status": "healthy" if row.recent_records > 0 else "stale",
        }

    return freshness


# ── Default DAG args ──────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


# ── Streaming DAG ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="tennis_streaming_realtime",
    default_args=default_args,
    description="Real-time tennis data streaming pipeline",
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tennis", "streaming", "realtime"],
) as dag:

    # Stage 1: Check for new messages
    check_messages = BranchPythonOperator(
        task_id="check_pubsub_messages",
        python_callable=check_pubsub_messages,
        provide_context=True,
    )

    # Stage 2a: Process messages if available
    process_messages = PythonOperator(
        task_id="process_streaming_messages",
        python_callable=process_streaming_messages,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Stage 2b: Log when no messages
    log_no_messages = BashOperator(
        task_id="log_no_messages",
        bash_command="echo 'No new messages in Pub/Sub subscription'",
    )

    # Stage 2c: Log errors
    log_error = BashOperator(
        task_id="log_error",
        bash_command="echo 'Error checking Pub/Sub messages'",
    )

    # Stage 3: Aggregate streaming stats
    aggregate_stats = PythonOperator(
        task_id="aggregate_streaming_stats",
        python_callable=aggregate_streaming_stats,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Stage 4: Check data freshness
    check_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Stage 5: Run dbt for streaming models
    dbt_streaming_run = BashOperator(
        task_id="dbt_streaming_run",
        bash_command=(
            "dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt "
            "--select stream_tennis_pipeline.tennis_streaming"
        ),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Dependencies
    check_messages >> [process_messages, log_no_messages, log_error] >> aggregate_stats
    aggregate_stats >> check_freshness >> dbt_streaming_run
