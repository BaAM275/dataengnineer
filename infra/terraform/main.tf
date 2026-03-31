resource "google_storage_bucket" "data_lake" {
  name                        = var.bucket_name
  location                    = var.region
  uniform_bucket_level_access = true

  # Keep it simple for course projects. You can add lifecycle rules later.
  force_destroy = false
}

resource "google_service_account" "pipeline" {
  account_id   = "tea-coffee-pipeline-sa"
  display_name = "Tea Coffee Pipeline Service Account"
}

resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id = var.bq_dataset
  location   = "US"

  # Allow dbt/Airflow to create tables.
  delete_contents_on_destroy = false
}

# Minimal permissions for pipeline tasks:
# - upload/read objects in the bucket
# - create/load tables and run jobs in BigQuery
resource "google_storage_bucket_iam_member" "bucket_objects" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "bq_editor" {
  dataset_id = google_bigquery_dataset.bq_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

