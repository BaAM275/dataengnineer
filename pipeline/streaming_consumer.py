"""
Streaming consumer that reads from Pub/Sub and writes to BigQuery.

This module provides both a standalone consumer script and helper functions
for the Airflow DAG to integrate streaming with batch processing.
"""

import json
import csv
from datetime import datetime
from typing import Generator, Optional
from pathlib import Path

from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud.bigquery import LoadJobConfig, QueryJobConfig, Schema
from google.cloud.bigquery.table import Table, TimePartitioning, ClusteringPolicy
from google.api_core.exceptions import NotFound
import os


def create_bigquery_tables_streaming(project_id: str, dataset_id: str) -> None:
    """
    Create BigQuery tables for streaming data if they don't exist.

    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
    """
    client = bigquery.Client(project=project_id)

    datasets = [ds.dataset_id for ds in client.list_datasets()]
    if dataset_id not in datasets:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset: {dataset_id}")

    tables = {
        "realtime_matches": _get_realtime_matches_schema(),
        "match_stats_streaming": _get_match_stats_schema(),
    }

    for table_id, schema in tables.items():
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        try:
            client.get_table(table_ref)
            print(f"Table {table_ref} already exists")
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="ingested_at",
            )
            table.clustering_fields = (
                ["tour", "tourney_year"]
                if table_id == "realtime_matches"
                else ["tour", "surface"]
            )
            table.labels = {
                "environment": "production",
                "pipeline": "tennis-data-pipeline",
            }
            client.create_table(table)
            print(f"Created table: {table_ref}")


def _get_realtime_matches_schema() -> list[bigquery.SchemaField]:
    """Return schema for realtime_matches table."""
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tourney_year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("tourney_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tourney_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("tour", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("winner_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("winner_rank", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("winner_age", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("loser_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("loser_rank", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("loser_age", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("score", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("best_of", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("round", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("surface", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tourney_level", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("match_duration_minutes", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("w_ace", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("w_df", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("w_sv_gms", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("w_1st_in", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("w_1st_won", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("w_2nd_won", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("w_bp_save", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("w_bp_convert", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("l_ace", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("l_df", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("l_sv_gms", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("l_1st_in", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("l_1st_won", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("l_2nd_won", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("l_bp_save", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("l_bp_convert", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("data_source", "STRING", mode="NULLABLE"),
    ]


def _get_match_stats_schema() -> list[bigquery.SchemaField]:
    """Return schema for match_stats_streaming table."""
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tour", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("surface", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tourney_year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("total_aces", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("total_double_faults", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("winner_first_serve_pct", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("winner_first_serve_win_pct", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("winner_second_serve_win_pct", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("loser_first_serve_pct", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("loser_first_serve_win_pct", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("loser_second_serve_win_pct", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("total_break_points", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("break_point_conversion_pct", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("match_duration_minutes", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sets_played", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("games_played", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("tiebreaks_played", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("is_completed", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    ]


def insert_match_to_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
    match_data: dict,
) -> None:
    """
    Insert a single match record into BigQuery using streaming insert.

    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        match_data: Dictionary containing match data
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    match_data["ingested_at"] = datetime.utcnow().isoformat()

    errors = client.insert_rows_json(table_ref, [match_data])

    if errors:
        print(f"Errors inserting row: {errors}")
        raise Exception(f"Failed to insert row: {errors}")


def batch_insert_to_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
    matches: list[dict],
    chunk_size: int = 500,
) -> int:
    """
    Insert multiple match records into BigQuery using streaming inserts.

    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        matches: List of match dictionaries
        chunk_size: Number of rows per insert

    Returns:
        Number of rows inserted
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    ingested_at = datetime.utcnow().isoformat()
    for match in matches:
        match["ingested_at"] = ingested_at

    total_inserted = 0
    for i in range(0, len(matches), chunk_size):
        chunk = matches[i:i + chunk_size]
        errors = client.insert_rows_json(table_ref, chunk)

        if errors:
            print(f"Errors inserting chunk {i // chunk_size}: {errors}")
        else:
            total_inserted += len(chunk)

    return total_inserted


def load_csv_to_streaming_table(
    project_id: str,
    dataset_id: str,
    table_id: str,
    csv_path: Path,
) -> int:
    """
    Load CSV data into BigQuery streaming table.

    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        csv_path: Path to CSV file

    Returns:
        Number of rows loaded
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
    )

    with open(csv_path, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)

    job.result()
    return job.output_rows


def stream_csv_to_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
    csv_path: Path,
    chunk_size: int = 500,
) -> int:
    """
    Stream CSV data to BigQuery using streaming inserts (not batch load).
    This demonstrates the streaming pattern where data is sent row-by-row.

    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        csv_path: Path to CSV file
        chunk_size: Number of rows per streaming insert

    Returns:
        Number of rows streamed
    """
    matches = []

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            matches.append(row)

    return batch_insert_to_bigquery(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        matches=matches,
        chunk_size=chunk_size,
    )


def subscribe_and_process(
    project_id: str,
    subscription_id: str,
    dataset_id: str,
    table_id: str,
    timeout: int = 60,
) -> int:
    """
    Subscribe to Pub/Sub messages and stream them to BigQuery.

    This is a blocking function that processes messages as they arrive.

    Args:
        project_id: GCP project ID
        subscription_id: Pub/Sub subscription ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        timeout: How long to wait for messages (seconds)

    Returns:
        Number of messages processed
    """
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    message_count = 0
    batch = []

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        nonlocal batch, message_count

        try:
            data = json.loads(message.data.decode("utf-8"))
            data["data_source"] = "streaming"
            data["ingested_at"] = datetime.utcnow().isoformat()
            batch.append(data)

            if len(batch) >= 100:
                errors = client.insert_rows_json(table_ref, batch)
                if errors:
                    print(f"Errors: {errors}")
                else:
                    message_count += len(batch)
                    batch = []

            message.ack()
        except Exception as e:
            print(f"Error processing message: {e}")
            message.nack()

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback,
    )
    print(f"Listening for messages on {subscription_path}...")

    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()

    if batch:
        errors = client.insert_rows_json(table_ref, batch)
        if not errors:
            message_count += len(batch)

    return message_count


def create_streaming_dataset_views(project_id: str, dataset_id: str) -> None:
    """
    Create views in the streaming dataset for real-time analytics.

    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
    """
    client = bigquery.Client(project=project_id)

    views = {
        "v_realtime_age_stats": f"""
            SELECT
                tour,
                tourney_year,
                AVG(winner_age) as avg_winner_age,
                AVG(loser_age) as avg_loser_age,
                AVG(winner_age) - AVG(loser_age) as avg_age_gap,
                COUNT(*) as match_count
            FROM `{project_id}.{dataset_id}.realtime_matches`
            WHERE winner_age IS NOT NULL AND loser_age IS NOT NULL
            GROUP BY tour, tourney_year
        """,
        "v_realtime_match_stats": f"""
            SELECT
                tour,
                surface,
                tourney_year,
                AVG(total_aces) as avg_aces,
                AVG(total_double_faults) as avg_double_faults,
                AVG(match_duration_minutes) as avg_match_duration,
                COUNT(*) as match_count
            FROM `{project_id}.{dataset_id}.match_stats_streaming`
            GROUP BY tour, surface, tourney_year
        """,
    }

    for view_name, query in views.items():
        view_ref = f"{project_id}.{dataset_id}.{view_name}"

        try:
            client.get_table(view_ref)
            print(f"View {view_ref} already exists, updating...")
            view = client.get_table(view_ref)
            view.query = query
            client.update_table(view, ["query"])
        except NotFound:
            view = bigquery.Table(view_ref)
            view.view_query = query
            view.labels = {
                "environment": "production",
                "pipeline": "tennis-data-pipeline",
            }
            client.create_table(view)
            print(f"Created view: {view_ref}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Stream tennis data to BigQuery")
    parser.add_argument("--project-id", required=True, help="GCP project ID")
    parser.add_argument("--dataset-id", default="tennis_streaming", help="BigQuery dataset ID")
    parser.add_argument("--csv-path", help="Path to CSV file to stream")
    parser.add_argument("--create-tables", action="store_true", help="Create tables first")

    args = parser.parse_args()

    if args.create_tables:
        create_bigquery_tables_streaming(args.project_id, args.dataset_id)
        create_streaming_dataset_views(args.project_id, args.dataset_id)
        print("Tables and views created successfully")

    if args.csv_path:
        count = stream_csv_to_bigquery(
            project_id=args.project_id,
            dataset_id=args.dataset_id,
            table_id="realtime_matches",
            csv_path=Path(args.csv_path),
        )
        print(f"Streamed {count} rows to BigQuery")
