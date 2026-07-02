FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
# install ingestion library from the repo before the rest
COPY --chown=airflow:root ../ingestion /opt/f1-ingestion/ingestion
COPY --chown=airflow:root ../pyproject.toml /opt/f1-ingestion/pyproject.toml
RUN pip install --no-cache-dir /opt/f1-ingestion && \
    pip install --no-cache-dir -r /requirements.txt
