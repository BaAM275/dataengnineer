output "gcs_bucket_name" {
  value       = google_storage_bucket.data_lake.name
  description = "Data lake bucket name"
}

output "bq_dataset_id" {
  value       = google_bigquery_dataset.bq_dataset.dataset_id
  description = "BigQuery dataset id"
}

output "pipeline_service_account_email" {
  value       = google_service_account.pipeline.email
  description = "Service account email for pipeline (upload to GCS, load tables in BigQuery)"
}

