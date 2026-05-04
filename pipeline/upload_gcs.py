from pathlib import Path
from google.cloud import storage


def upload_to_gcs(
    local_path: str | Path,
    bucket_name: str,
    gcs_prefix: str = "raw",
) -> str:
    """
    Upload a local file to GCS.

    Args:
        local_path: path to the local file
        bucket_name: GCS bucket name
        gcs_prefix: folder prefix inside the bucket (default: "raw")

    Returns:
        GCS URI of the uploaded file (gs://bucket/path)
    """
    local_path = Path(local_path)
    blob_name = f"{gcs_prefix}/{local_path.name}"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_filename(local_path)
    gcs_uri = f"gs://{bucket_name}/{blob_name}"
    print(f"Uploaded {local_path} -> {gcs_uri}")
    return gcs_uri

def upload_many_to_gcs(
    local_paths: list[Path | str],
    bucket_name: str,
    gcs_prefix: str = "raw",
) -> list[str]:
    """
    Upload multiple local files to GCS.

    Args:
        local_paths: list of local file paths
        bucket_name: GCS bucket name
        gcs_prefix: folder prefix inside the bucket (default: "raw")

    Returns:
        List of GCS URIs
    """
    return [
        upload_to_gcs(local_path, bucket_name, gcs_prefix)
        for local_path in local_paths
    ]