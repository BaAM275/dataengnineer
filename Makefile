.PHONY: install install-dev test dashboard upload load dbt

install:
	pip install -r requirements.txt

install-dev:
	pip install -r dev-requirements.txt

test:
	pytest -q

dashboard:
	streamlit run dashboard/app.py

upload:
	python scripts/upload_to_gcs.py

load:
	python scripts/load_gcs_csv_to_bigquery.py

dbt:
	dbt run --project-dir dbt --profiles-dir dbt --target prod

