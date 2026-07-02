"""Utilities for calling the Microsoft Fabric REST API from Airflow DAGs."""

import os
import time
import requests
from azure.identity import DefaultAzureCredential


FABRIC_API = "https://api.fabric.microsoft.com/v1"
TOKEN_SCOPE = "https://api.fabric.microsoft.com/.default"


def get_fabric_token() -> str:
    # DefaultAzureCredential picks up az login automatically in local dev.
    # In production with a Service Principal, set AZURE_TENANT_ID, AZURE_CLIENT_ID,
    # AZURE_CLIENT_SECRET as env vars and it will use those instead.
    credential = DefaultAzureCredential()
    token = credential.get_token(TOKEN_SCOPE)
    return token.token


def run_notebook(workspace_id: str, notebook_id: str, parameters: dict | None = None) -> str:
    """Trigger a Fabric notebook and return the job instance ID."""
    token = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    body = {}
    if parameters:
        body["executionData"] = {"parameters": parameters}

    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
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

        if status in ("Succeeded", "Failed", "Cancelled"):
            return status

        time.sleep(poll_interval)
        elapsed += poll_interval

        # refresh token every ~45 min to avoid expiry
        if elapsed % 2700 == 0:
            token = get_fabric_token()

    raise TimeoutError(f"Notebook job did not complete within {timeout}s")


def get_storage_options() -> dict:
    # Uses DefaultAzureCredential — works with az login for local dev.
    # For CI/CD add AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET as env vars.
    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default").token
    return {
        "account_name": "onelake",
        "bearer_token": token,
    }
