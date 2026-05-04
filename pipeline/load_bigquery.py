import json
from pathlib import Path
from google.cloud import bigquery


SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "matches.json"


def load_schema() -> list[bigquery.SchemaField]:
    """Load BigQuery schema from JSON file."""
    with open(SCHEMA_PATH) as f:
        fields = json.load(f)
    return [
        bigquery.SchemaField(
            name=field["name"],
            field_type=field["type"],
            mode=field["mode"],
        )
        for field in fields
    ]


def load_gcs_to_bigquery(
    gcs_uri: str,
    dataset_id: str,
    table_id: str,
    project_id: str,
) -> None:
    """
    Load a CSV file from GCS into a BigQuery table.

    Args:
        gcs_uri: GCS URI of the file (gs://bucket/path/file.csv)
        dataset_id: BigQuery dataset ID (e.g. "tennis_raw")
        table_id: BigQuery table ID (e.g. "atp_matches")
        project_id: GCP project ID
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=load_schema(),
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config,
    )
    load_job.result()  # wait for the job to complete
    print(f"Loaded {gcs_uri} -> {table_ref}")


def load_tour_to_bigquery(
    tour: str,
    bucket_name: str,
    dataset_id: str,
    project_id: str,
    gcs_prefix: str = "raw",
) -> None:
    """
    Load all CSVs for a tour from GCS into BigQuery using a wildcard URI.

    Args:
        tour: "atp" or "wta"
        bucket_name: GCS bucket name
        dataset_id: BigQuery dataset ID
        project_id: GCP project ID
        gcs_prefix: folder prefix inside the bucket (default: "raw")
    """
    gcs_uri = f"gs://{bucket_name}/{gcs_prefix}/{tour.lower()}_matches_*.csv"
    table_id = f"{tour.lower()}_matches"

    load_gcs_to_bigquery(
        gcs_uri=gcs_uri,
        dataset_id=dataset_id,
        table_id=table_id,
        project_id=project_id,
    )