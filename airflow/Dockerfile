FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        git curl ca-certificates lsb-release gnupg && \
    curl -sLS https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg && \
    echo "deb [arch=amd64,arm64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" \
        > /etc/apt/sources.list.d/azure-cli.list && \
    apt-get update && apt-get install -y --no-install-recommends azure-cli && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# copy ingestion library and install it first
COPY --chown=airflow:root ingestion /opt/f1-ingestion/ingestion
COPY --chown=airflow:root pyproject.toml /opt/f1-ingestion/pyproject.toml

RUN pip install --no-cache-dir /opt/f1-ingestion

# install airflow extra dependencies
COPY --chown=airflow:root airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
