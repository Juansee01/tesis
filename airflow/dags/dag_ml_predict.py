"""
DAG: dag_ml_predict
Triggers the Fabric ML inference notebook after dag_transform_gold finishes.
Reads mart_pitstop_features from Gold, loads the production model from
Fabric ML Experiments, writes predictions to mart_pitstop_predictions.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from fabric_utils import run_notebook, wait_for_notebook

default_args = {
    "owner": "juan_lizarralde",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["juanlizarralde@turismocity.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def trigger_inference_notebook(**context):
    workspace_id = os.environ["FABRIC_WORKSPACE_ID"]
    notebook_id = os.environ["NOTEBOOK_ID_ML_INFER"]

    job_location = run_notebook(workspace_id, notebook_id)
    print(f"Inference notebook triggered — polling: {job_location}")

    status = wait_for_notebook(job_location, poll_interval=20, timeout=600)

    if status != "Succeeded":
        raise RuntimeError(f"Inference notebook failed with status: {status}")

    print("Predictions written to mart_pitstop_predictions in Gold")


with DAG(
    dag_id="dag_ml_predict",
    default_args=default_args,
    description="Run ML inference: predict pit stop window for latest GP",
    schedule="0 13 * * MON",  # after dag_validate
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ml", "inference", "gold"],
) as dag:

    predict = PythonOperator(
        task_id="trigger_ml_inference_notebook",
        python_callable=trigger_inference_notebook,
    )
