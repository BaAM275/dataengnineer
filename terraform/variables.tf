variable "project_id" {
  description = "GCP project ID"
}

variable "region" {
  description = "GCP region"
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Name of the GCS bucket for raw data"
}

variable "credentials_file" {
  description = "Path to the GCP service account key file"
}

variable "environment" {
  description = "Environment name (e.g., production, staging)"
  default     = "production"
}

variable "enable_streaming" {
  description = "Enable streaming infrastructure (Pub/Sub, Dataflow)"
  default     = true
}

variable "service_account_email" {
  description = "Email of the service account for IAM roles"
  default     = ""
}

variable "gcs_temp_bucket" {
  description = "GCS bucket for Dataflow temporary files"
  default     = ""
}