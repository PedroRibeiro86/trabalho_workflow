FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.13.0 \
    requests==2.32.3
