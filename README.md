# Tennis Data Pipeline

An end-to-end batch and streaming data pipeline built on ATP and WTA tennis data (Jeff Sackmann, GitHub CSVs). Built as a final project for the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) 2026 cohort.

## Research Questions

- How has the age of successful players changed over time? (ATP vs WTA comparison)
- Match outcomes — likelihood of going to a deciding set (best-of-3 matches)
- Real-time serve statistics and match analysis (streaming)

## Dashboard

The Looker Studio dashboard is available at: https://lookerstudio.google.com/s/mBMWQxVjtaI

### Dashboard Tiles (4 tiles)

1. **Age Trends Over Time** - Line chart showing average winner/loser age by year (ATP vs WTA)
2. **Deciding Set Likelihood** - Bar chart showing probability of going to 3rd set in best-of-3 matches
3. **Match Duration Analysis** - Scatter plot of match duration vs surface type
4. **Real-time Serve Statistics** - Table showing top serve performers (aces, win rates) from streaming data

## Architecture

```
GitHub CSVs (ATP + WTA)
        │
        ▼
   Airflow DAG (Docker Compose)
        │
        ├─────────────────────────────────────────────────────────────────────┐
        │                         BATCH PIPELINE                              │
        ├─────────────────────────────────────────────────────────────────────┤
        │  setup_group                                                        │
        │  ├── validate_config                                                │
        │  └── create_streaming_infrastructure                                │
        │                                                                      │
        │  atp_batch_group / wta_batch_group                                 │
        │  ├── cleanup_gcs                                                    │
        │  ├── download_{year} (x30)                                          │
        │  ├── upload_{year} (x30)                                            │
        │  └── load_bigquery                                                  │
        │                                                                      │
        │  streaming_group                                                    │
        │  ├── publish_atp_to_pubsub                                          │
        │  ├── publish_wta_to_pubsub                                          │
        │  ├── stream_atp_to_bigquery                                          │
        │  ├── stream_wta_to_bigquery                                          │
        │  └── validate_streaming_data                                         │
        │                                                                      │
        │  quality_group                                                      │
        │  ├── check_batch_row_counts                                         │
        │  ├── check_streaming_row_counts                                     │
        │  └── check_data_freshness                                          │
        │                                                                      │
        │  dbt_deps → dbt_run → dbt_test → notification_group                │
        └─────────────────────────────────────────────────────────────────────┘
        │
        ▼
   Google Cloud Pub/Sub (Streaming)
   ├── tennis-matches-stream (main topic)
   └── tennis-matches-stream-dlq (dead letter queue)
        │
        ▼
   Google Dataflow (Streaming Pipeline)
   └── Pub/Sub → BigQuery Streaming Inserts
        │
        ▼
   BigQuery Datasets
   ├── tennis_raw (staging data)
   ├── tennis_prod (transformed data via dbt)
   └── tennis_streaming (real-time data)
        │
        ▼
   Looker Studio Dashboard
```

**Stack:**
- **IaC:** Terraform — provisions GCS buckets, BigQuery datasets, Pub/Sub topics, Dataflow jobs, IAM roles
- **Orchestration:** Airflow 3 via Docker Compose (GitHub Codespaces)
- **Streaming:** Google Cloud Pub/Sub with Dataflow streaming pipeline
- **Data lake:** Google Cloud Storage (GCS)
- **Warehouse:** BigQuery (partitioned by year, clustered by tour)
- **Transformations:** dbt Core (staging → intermediate → marts → streaming marts)
- **Dashboard:** Looker Studio (4 tiles)
- **Monitoring:** Data freshness checks, quality tests

## Project Structure

```
tennis-data-pipeline/
├── .devcontainer/
│   ├── devcontainer.json       # Codespaces environment definition
│   └── install.sh              # installs gcloud, uv; configures auth and pip packages
├── airflow/
│   ├── Dockerfile              # extends apache/airflow:3.1.8
│   ├── docker-compose.yaml
│   ├── .env.example            # template for required environment variables
│   ├── requirements.txt        # pipeline + dbt dependencies (includes google-cloud-pubsub)
│   └── dags/
│       ├── tennis_pipeline.py      # main batch + streaming DAG
│       └── tennis_streaming_realtime.py  # real-time streaming DAG
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml            # dbt connection profile (uses env vars, safe to commit)
│   ├── packages.yml
│   ├── sources/
│   │   └── tennis_streaming.yml   # source definitions for streaming tables
│   ├── models/
│   │   ├── staging/            # stg_atp_matches, stg_wta_matches
│   │   ├── intermediate/       # int_matches_unionized, int_matches_enriched
│   │   ├── marts/              # fct_player_age_trends, fct_match_outcomes
│   │   ├── streaming_staging/  # stg_realtime_matches
│   │   └── streaming_marts/    # fct_realtime_age_trends, fct_realtime_serve_stats
│   └── tests/                  # singular data tests
├── pipeline/
│   ├── download.py             # download CSVs from GitHub
│   ├── upload_gcs.py           # upload files to GCS
│   ├── load_bigquery.py        # load GCS files into BigQuery
│   ├── streaming_producer.py   # publish data to Pub/Sub
│   └── streaming_consumer.py  # consume from Pub/Sub and stream to BigQuery
├── schemas/
│   ├── matches.json            # BigQuery schema for raw tables
│   ├── matches_streaming.json  # BigQuery schema for streaming tables
│   └── match_stats_streaming.json
├── terraform/
│   ├── main.tf                 # GCS, BigQuery, Pub/Sub, Dataflow, IAM resources
│   ├── variables.tf
│   └── terraform.tfvars.example
├── Makefile                    # convenience targets for setup
└── README.md
```

---

## Reproducing This Project

### Prerequisites

- A [GitHub](https://github.com) account with Codespaces access
- A [Google Cloud Platform](https://console.cloud.google.com) account (free tier is sufficient)

---

### Step 1 — Fork the repository

Fork this repository to your own GitHub account so you can create Codespaces secrets against it.

---

### Step 2 — Create a GCP project

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create a new project — note the **Project ID**
3. Enable the following APIs:
   - [Cloud Resource Manager API](https://console.developers.google.com/apis/api/cloudresourcemanager.googleapis.com)
   - [Cloud Storage API](https://console.developers.google.com/apis/api/storage.googleapis.com)
   - [BigQuery API](https://console.developers.google.com/apis/api/bigquery.googleapis.com)
   - [Pub/Sub API](https://console.developers.google.com/apis/api/pubsub.googleapis.com)
   - [Dataflow API](https://console.developers.google.com/apis/api/dataflow.googleapis.com)
   - [Cloud Scheduler API](https://console.developers.google.com/apis/api/cloudscheduler.googleapis.com)

---

### Step 3 — Create a service account

1. Go to **IAM & Admin → Service Accounts → Create Service Account**
2. Name it `terraform-sa`
3. Assign the following roles:
   - `Storage Admin`
   - `BigQuery Admin`
   - `Service Usage Consumer`
   - `Pub/Sub Admin`
   - `Dataflow Admin`
   - `Cloud Scheduler Admin`
4. Go to the **Keys** tab → **Add Key → Create new key → JSON**
5. Download the JSON key file

> ⚠️ Never commit the JSON key file to the repository.

---

### Step 4 — Add the service account key as a Codespaces secret

1. Go to **github.com → Settings → Codespaces → New secret**
2. Name: `GOOGLE_APPLICATION_CREDENTIALS_JSON`
3. Value: paste the **full contents** of the JSON key file
4. Repository access: select your forked `tennis-data-pipeline` repository

---

### Step 5 — Start the Codespace

1. Open the forked repository on GitHub
2. Click **Code → Codespaces → New codespace**
3. Wait for the environment to build — this takes a few minutes on first run

The devcontainer automatically:
- Installs Terraform, Google Cloud CLI (`gcloud`), and `uv`
- Authenticates with GCP using the secret from Step 4
- Installs recommended VS Code extensions (dbt Power User, Makefile Tools)

**Open a new terminal** after the build completes to ensure all environment variables are loaded. You should see:
```
GCP authentication complete.
```

Verify the setup:
```bash
terraform version
gcloud version
gcloud projects describe <your-project-id>
```

---

### Step 6 — Configure variables

**Terraform:**
```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
```

Edit `terraform/terraform.tfvars`:
```hcl
project_id            = "your-gcp-project-id"
region                = "us-central1"
bucket_name           = "your-gcp-bucket-name"
credentials_file      = "/tmp/gcp-key.json"
environment           = "production"
enable_streaming      = true
service_account_email = "terraform-sa@your-project-id.iam.gserviceaccount.com"
gcs_temp_bucket      = "gs://your-gcp-bucket-name-dataflow-temp"
```

**Airflow:**
```bash
cp airflow/.env.example airflow/.env
```

Edit `airflow/.env`:
```bash
GCP_PROJECT_ID=your-gcp-project-id
GCS_BUCKET=your-unique-bucket-name
BQ_STREAMING_DATASET=tennis_streaming
PUBSUB_TOPIC=tennis-matches-stream
PUBSUB_SUBSCRIPTION=tennis-matches-subscription
```

---

### Step 7 — Provision infrastructure and start the pipeline

#### Option A — Using Make (recommended)
```bash
make infra    # provision GCS bucket + BigQuery datasets
make airflow  # build and start Airflow
make dbt      # install dbt packages and verify connection
```

Or run everything in one command:
```bash
make all
```

Run `make help` to see all available targets.

#### Option B — Manual steps

**Provision infrastructure:**
```bash
cd terraform
terraform init
terraform plan
terraform apply
```

**Start Airflow:**
```bash
cd airflow
docker compose up --build -d
```

Wait ~2 minutes for services to become healthy:
```bash
docker compose ps
```

The `airflow-apiserver` and `airflow-scheduler` should show `(healthy)`.

**Install dbt packages:**
```bash
docker compose exec airflow-scheduler \
  dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

---

### Step 8 — Open the Airflow UI

Open the Airflow UI via the **Ports** tab in Codespaces (port 8080). Log in with `airflow` / `airflow`.

Go to **Admin → Variables** and create:

| Key | Value | Description |
|-----|-------|-------------|
| `tennis_start_year` | `1995` | First year to download |
| `tennis_end_year` | `2024` | Last year to download |

---

### Step 9 — Trigger the pipeline

Enable and trigger the `tennis_pipeline` DAG in the Airflow UI. The full run for 30 years of data takes approximately 20-30 minutes.

Once complete, verify in BigQuery:
- `tennis_raw.atp_matches` and `tennis_raw.wta_matches` should exist with data
- `tennis_prod` should contain the dbt views and tables

---

### Step 10 — View the dashboard

The Looker Studio dashboard is available at: https://lookerstudio.google.com/s/mBMWQxVjtaI

---

## Restarting the Codespace

After a Codespace **restart** (not rebuild), GCP auth is re-applied automatically via `postStartCommand`. Open a new terminal to ensure `~/.bashrc` is sourced, then restart Airflow:
```bash
cd airflow
docker compose up -d
```

---

## License

MIT License
