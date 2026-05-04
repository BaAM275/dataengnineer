AIRFLOW_DIR=airflow
TERRAFORM_DIR=terraform

.PHONY: all infra airflow dbt check-env help

## Run the full pipeline setup
all: check-env infra airflow dbt
	@echo "✅ Setup complete. Trigger the DAG in the Airflow UI to run the pipeline."

## Check required files exist before proceeding
check-env:
	@test -f $(AIRFLOW_DIR)/.env || \
		(echo "❌ ERROR: airflow/.env not found." && \
		echo "   Run: cp airflow/.env.example airflow/.env" && \
		echo "   Then fill in your GCP project ID, bucket name, and other values." && \
		exit 1)
	@test -f $(TERRAFORM_DIR)/terraform.tfvars || \
		(echo "❌ ERROR: terraform/terraform.tfvars not found." && \
		echo "   Run: cp terraform/terraform.tfvars.example terraform/terraform.tfvars" && \
		echo "   Then fill in your GCP project ID, region, and bucket name." && \
		exit 1)

## Provision GCS bucket and BigQuery datasets via Terraform
infra: check-env
	@echo "🔧 Provisioning cloud infrastructure..."
	cd $(TERRAFORM_DIR) && terraform init && terraform plan && terraform apply -auto-approve
	@echo "✅ Infrastructure ready."

## Build and start Airflow
airflow: check-env
	@echo "🚀 Starting Airflow..."
	cd $(AIRFLOW_DIR) && docker compose up --build -d
	@echo "⏳ Waiting for Airflow to become healthy (this takes ~2 minutes)..."
	@sleep 120
	@echo "✅ Airflow is running."
	@echo "   Open the UI at: http://localhost:8080 (or via Codespaces Ports tab)"
	@echo "   Login: airflow / airflow"
	@echo "   Set Airflow Variables: tennis_start_year=1995, tennis_end_year=2024"

## Install dbt packages and verify connection
dbt: check-env
	@echo "📦 Installing dbt packages..."
	cd $(AIRFLOW_DIR) && docker compose exec airflow-scheduler \
		dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
	@echo "🔍 Verifying dbt connection..."
	cd $(AIRFLOW_DIR) && docker compose exec airflow-scheduler \
		dbt debug --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
	@echo "✅ dbt ready."

## Show available targets
help:
	@echo "Available targets:"
	@echo "  make all       — full setup (infra + airflow + dbt)"
	@echo "  make infra     — provision GCS and BigQuery via Terraform"
	@echo "  make airflow   — build and start Airflow"
	@echo "  make dbt       — install dbt packages and verify connection"
	@echo "  make help      — show this message"