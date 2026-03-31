variable "gcp_project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  description = "GCP region"
  default     = "us-central1"
}

variable "bucket_name" {
  type        = string
  description = "Name of the GCS bucket used as data lake (must be globally unique)"
}

variable "bq_dataset" {
  type        = string
  description = "BigQuery dataset for raw + curated models"
}

