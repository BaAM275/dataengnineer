FROM apache/airflow:3.1.8

USER airflow
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt
ENV PATH="/home/airflow/.local/bin:${PATH}"