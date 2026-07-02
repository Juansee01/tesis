"""
DAG: dag_transform_silver
Triggers the Fabric PySpark notebook that transforms Bronze → Silver.
The notebook handles cleaning, normalization, and enrichment.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from fabric_utils import run_notebook, wait_for_notebook

default_args = {
    "owner": "juan_lizarralde",
    "depends_on_past": True,  # silver depends on previous run completing successfully
    "email_on_failure": True,
    "email": ["juanlizarralde@turismocity.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def trigger_silver_notebook(**context):
    workspace_id = os.environ["FABRIC_WORKSPACE_ID"]
    notebook_id = os.environ["NOTEBOOK_ID_BRONZE_TO_SILVER"]

    execution_date = context["execution_date"]
    params = {
        "execution_date": execution_date.isoformat(),
    }

    job_location = run_notebook(workspace_id, notebook_id, parameters=params)
    print(f"Silver notebook triggered — polling job at: {job_location}")

    status = wait_for_notebook(job_location, poll_interval=30, timeout=3600)

    if status != "Succeeded":
        raise RuntimeError(f"Silver notebook failed with status: {status}")

    print(f"Silver transformation complete — status: {status}")


with DAG(
    dag_id="dag_transform_silver",
    default_args=default_args,
    description="Trigger Fabric notebook: Bronze → Silver transformation",
    schedule="0 9 * * MON",  # after ingestion DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["transformation", "silver"],
) as dag:

    transform_silver = PythonOperator(
        task_id="trigger_bronze_to_silver_notebook",
        python_callable=trigger_silver_notebook,
    )
