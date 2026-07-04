"""Utilities for calling the Microsoft Fabric REST API from Airflow DAGs."""

import os
import time
import requests
from azure.identity import AzureCliCredential, ChainedTokenCredential, ManagedIdentityCredential


FABRIC_API = "https://api.fabric.microsoft.com/v1"
TOKEN_SCOPE = "https://api.fabric.microsoft.com/.default"


def _get_credential():
    # Local dev: picks up az login from ~/.azure (mounted into the container)
    # Production on ACI with managed identity: ManagedIdentityCredential kicks in
    return ChainedTokenCredential(AzureCliCredential(), ManagedIdentityCredential())


def get_fabric_token() -> str:
    token = _get_credential().get_token(TOKEN_SCOPE)
    return token.token


def run_notebook(workspace_id: str, notebook_id: str, parameters: dict | None = None) -> str:
    """Trigger a Fabric notebook and return the job instance ID."""
    token = get_fabric_token()
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body: dict = {"executionData": {}}
    if parameters:
        body["executionData"] = {"parameters": parameters}
    resp = requests.post(url, headers=headers, json=body, timeout=30)
    resp.raise_for_status()

    # Fabric returns 202 with Location header pointing to the job instance
    location = resp.headers.get("Location", "")
    return location


def wait_for_notebook(job_location: str, poll_interval: int = 30, timeout: int = 3600) -> str:
    """Poll the job status until it completes or times out. Returns final status."""
    token = get_fabric_token()
    elapsed = 0

    while elapsed < timeout:
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(job_location, headers=headers, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        status = data.get("status", "")

        if status in ("Succeeded", "Completed", "Failed", "Cancelled", "DeadLettered"):
            return status

        time.sleep(poll_interval)
        elapsed += poll_interval

        # refresh token every ~45 min to avoid expiry
        if elapsed % 2700 == 0:
            token = get_fabric_token()

    raise TimeoutError(f"Notebook job did not complete within {timeout}s")


def get_storage_options() -> dict:
    token = _get_credential().get_token("https://storage.azure.com/.default").token
    return {
        "account_name": "onelake",
        "bearer_token": token,
    }


# Fabric Warehouse tables (Gold) are not reachable via ABFSS/Delta like the Lakehouse;
# they are only exposed through the T-SQL endpoint. Query them with pyodbc using an
# AAD access token from the same credential chain (az login locally / MSI on ACI).
SQL_COPT_SS_ACCESS_TOKEN = 1256  # pyodbc attr to pass an AAD access token
WAREHOUSE_TOKEN_SCOPE = "https://database.windows.net/.default"


def get_warehouse_connection(database: str | None = None):
    """Return a pyodbc connection to the Fabric Warehouse SQL endpoint."""
    import struct
    import pyodbc

    host = os.environ["FABRIC_SQL_HOST"]
    db = database or os.environ.get("FABRIC_WAREHOUSE", "f1_warehouse")

    token = _get_credential().get_token(WAREHOUSE_TOKEN_SCOPE).token
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host},1433;DATABASE={db};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    return pyodbc.connect(
        conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}, timeout=60
    )
