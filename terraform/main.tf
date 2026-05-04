terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "raw_data" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  # Uncomment to automatically delete files older than 30 days
  # lifecycle_rule {
  #   condition {
  #     age = 30
  #   }
  #   action {
  #     type = "Delete"
  #   }
  # }
}

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "tennis_raw"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "prod" {
  dataset_id                 = "tennis_prod"
  location                   = var.region
  delete_contents_on_destroy = true
}

# ─── Streaming Infrastructure (Pub/Sub) ────────────────────────────────────

resource "google_pubsub_topic" "tennis_matches" {
  name = "tennis-matches-stream"
  labels = {
    environment = "production"
    pipeline    = "tennis-data-pipeline"
  }
}

resource "google_pubsub_topic" "tennis_matches_dlq" {
  name = "tennis-matches-stream-dlq"
  labels = {
    environment = "production"
    pipeline    = "tennis-data-pipeline"
  }
}

resource "google_pubsub_subscription" "tennis_matches_subscription" {
  name  = "tennis-matches-subscription"
  topic = google_pubsub_topic.tennis_matches.name

  ack_deadline_seconds = 60

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.tennis_matches_dlq.id
    max_delivery_attempts = 5
  }

  expiration_policy {
    ttl = "" # Never expires
  }

  enable_exactly_once_delivery = true

  labels = {
    environment = "production"
    pipeline    = "tennis-data-pipeline"
  }
}

# ─── BigQuery Streaming Dataset ─────────────────────────────────────────────

resource "google_bigquery_dataset" "streaming" {
  dataset_id                 = "tennis_streaming"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "realtime_matches" {
  dataset_id = google_bigquery_dataset.streaming.dataset_id
  table_id   = "realtime_matches"

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  clustering = ["tour", "tourney_year"]

  schema = file("${path.module}/../schemas/matches_streaming.json")

  labels = {
    environment = "production"
    pipeline    = "tennis-data-pipeline"
  }
}

resource "google_bigquery_table" "match_stats_streaming" {
  dataset_id = google_bigquery_dataset.streaming.dataset_id
  table_id   = "match_stats_streaming"

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  clustering = ["tour", "surface"]

  schema = file("${path.module}/../schemas/match_stats_streaming.json")

  labels = {
    environment = "production"
    pipeline    = "tennis-data-pipeline"
  }
}

# ─── Dataflow Job (Streaming to BigQuery) ───────────────────────────────────

resource "google_dataflow_job" "streaming_pipeline" {
  count      = var.enable_streaming ? 1 : 0
  name       = "tennis-streaming-pipeline-${var.environment}"
  zone       = "${var.region}-a"
  template   = "gs://dataflow-templates-${var.region}/latest/PubSub_Subscription_to_BigQuery"
  temp_location = "${var.gcs_temp_bucket}/temp"
  machine_type = "n1-standard-2"

  parameters = {
    inputSubscription       = google_pubsub_subscription.tennis_matches_subscription.id
    outputTableSpec         = "${var.project_id}:${google_bigquery_dataset.streaming.dataset_id}.realtime_matches"
    bigQueryWriteMethod     = "STREAMING_INSERTS"
    deadLetterQueueTable    = "${var.project_id}:${google_bigquery_dataset.streaming.dataset_id}.realtime_matches_dlq"
    JavascriptUdfPath       = ""
    javascriptFunctionName  = ""
  }

  labels = {
    environment = "production"
    pipeline    = "tennis-data-pipeline"
  }

  on_delete = "cancel"
}

# ─── Cloud Scheduler for Batch to Streaming Bridge ──────────────────────────

resource "google_cloud_scheduler_job" "pubsub_publisher_daily" {
  count     = var.enable_streaming ? 1 : 0
  name      = "tennis-pubsub-publisher-daily"
  schedule  = "0 2 * * *" # Run daily at 2 AM
  time_zone = "UTC"
  region    = var.region

  pubsub_target {
    topic_name = "${google_pubsub_topic.tennis_matches.id}"
    data       = base64encode(jsonencode({
      trigger_type = "batch_to_stream"
      timestamp     = timestamp()
      source        = "airflow_batch_pipeline"
    }))
    attributes = {
      pipeline_version = "2.0"
      environment      = "production"
    }
  }

  retry_config {
    retry_count          = 3
    min_backoff_duration = "5s"
    max_backoff_duration = "300s"
  }
}

# ─── GCS Bucket for Dataflow Temp ───────────────────────────────────────────

resource "google_storage_bucket" "dataflow_temp" {
  count      = var.enable_streaming ? 1 : 0
  name       = "${var.bucket_name}-dataflow-temp"
  location   = var.region
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 7 # Delete temp files after 7 days
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = "production"
    pipeline    = "tennis-data-pipeline"
  }
}

# ─── IAM Roles for Streaming ─────────────────────────────────────────────────

resource "google_project_iam_member" "dataflow_worker" {
  count   = var.enable_streaming ? 1 : 0
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${var.service_account_email}"
}

resource "google_project_iam_member" "pubsub_publisher" {
  count   = var.enable_streaming ? 1 : 0
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${var.service_account_email}"
}

resource "google_project_iam_member" "pubsub_subscriber" {
  count   = var.enable_streaming ? 1 : 0
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${var.service_account_email}"
}