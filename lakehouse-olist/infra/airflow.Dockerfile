FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

COPY infra/airflow-requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
