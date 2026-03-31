from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path

from google.cloud import storage


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion-date", default=str(date.today()), help="YYYY-MM-DD")
    args = parser.parse_args()

    local_path = Path(_require_env("DATASET_LOCAL_PATH"))
    if not local_path.exists():
        raise FileNotFoundError(f"CSV not found: {local_path}")

    bucket_name = _require_env("GCS_BUCKET")

    object_path = (
        f"raw/tea_vs_coffee_global_final/ingestion_date={args.ingestion_date}/"
        f"tea_vs_coffee_global_final.csv"
    )

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_filename(str(local_path))

    print(f"Uploaded to gs://{bucket_name}/{object_path}")


if __name__ == "__main__":
    main()

