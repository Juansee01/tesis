FROM apache/airflow:2.9.3-python3.11

USER root
# Microsoft repo (debian 12 / bookworm) provides both azure-cli and the ODBC driver.
# ODBC Driver 18 for SQL Server is required by dbt-fabric to reach the Fabric Warehouse.
# Single explicit source with its own keyring; wipe any pre-existing MS lists to avoid
# "Conflicting values set for option Signed-By" on the prod repo.
RUN apt-get update && apt-get install -y --no-install-recommends \
        git curl ca-certificates gnupg lsb-release && \
    rm -f /etc/apt/sources.list.d/*microsoft* /etc/apt/sources.list.d/*mssql* /etc/apt/sources.list.d/*azure-cli* && \
    install -d /etc/apt/keyrings && \
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft-prod.gpg && \
    echo "deb [arch=amd64,arm64 signed-by=/etc/apt/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/repos/azure-cli/ bookworm main" \
        > /etc/apt/sources.list.d/azure-cli.list && \
    echo "deb [arch=amd64,arm64 signed-by=/etc/apt/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/microsoft-prod.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        azure-cli msodbcsql18 unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*

# dbt in an isolated venv: dbt-fabric's deps conflict with airflow 2.9.3's pins
# (pip ResolutionTooDeep) if installed in the same env. The DAG calls `dbt` via
# subprocess, so it only needs the binary on PATH, not shared with airflow's python.
# networkx>=3 avoids `fractions.gcd` (removed in Py>=3.9) pulled by dbt-core.
RUN python3 -m venv /opt/dbt-venv && \
    /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-fabric==1.10.0" "networkx>=3,<4" && \
    ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt

USER airflow

# copy ingestion library and install it first
COPY --chown=airflow:root ingestion /opt/f1-ingestion/ingestion
COPY --chown=airflow:root pyproject.toml /opt/f1-ingestion/pyproject.toml

RUN pip install --no-cache-dir /opt/f1-ingestion

# install airflow extra dependencies (includes dbt-fabric for dag_transform_gold)
COPY --chown=airflow:root airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
