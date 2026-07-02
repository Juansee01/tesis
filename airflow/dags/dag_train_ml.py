"""
DAG: dag_train_ml
Manually triggered (or monthly). Retrains the XGBoost pit stop classifier
using all available Gold data. Registers the model in Fabric ML Experiments
only if F1-score >= 0.65 on the 2024 test set.
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
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def trigger_training_notebook(**context):
    workspace_id = os.environ["FABRIC_WORKSPACE_ID"]
    notebook_id = os.environ["NOTEBOOK_ID_ML_TRAIN"]

    job_location = run_notebook(workspace_id, notebook_id)
    print(f"Training notebook triggered — polling: {job_location}")

    # training can take up to 15 min (GridSearchCV with 5-fold CV)
    status = wait_for_notebook(job_location, poll_interval=30, timeout=1800)

    if status != "Succeeded":
        raise RuntimeError(f"Training notebook failed with status: {status}")

    print("Training complete — check Fabric ML Experiments for the registered model and metrics")


with DAG(
    dag_id="dag_train_ml",
    default_args=default_args,
    description="Retrain XGBoost pit stop classifier and register in Fabric ML Experiments",
    schedule=None,  # manual trigger only (or set to "0 0 1 * *" for monthly)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ml", "training"],
) as dag:

    train = PythonOperator(
        task_id="trigger_training_notebook",
        python_callable=trigger_training_notebook,
    )
