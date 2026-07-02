"""
DAG: dag_transform_gold
Runs dbt models (Silver → Gold) by calling dbt run + dbt test
from the Airflow container (dbt-fabric connects via SQL endpoint).
"""

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

DBT_PROJECT_DIR = Path(__file__).parents[2] / "dbt"

default_args = {
    "owner": "juan_lizarralde",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["juanlizarralde@turismocity.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def run_dbt(command: list[str], **context):
    env = {
        **os.environ,
        "DBT_PROJECT_DIR": str(DBT_PROJECT_DIR),
    }
    result = subprocess.run(
        ["dbt", *command, "--project-dir", str(DBT_PROJECT_DIR)],
        capture_output=True,
        text=True,
        env=env,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt {' '.join(command)} failed:\n{result.stderr}")


with DAG(
    dag_id="dag_transform_gold",
    default_args=default_args,
    description="Run dbt models: Silver → Gold (dimensional model + ML features)",
    schedule="0 11 * * MON",  # after silver DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["transformation", "gold", "dbt"],
) as dag:

    dbt_run = PythonOperator(
        task_id="dbt_run_gold_models",
        python_callable=run_dbt,
        op_args=[["run", "--select", "gold"]],
    )

    dbt_test = PythonOperator(
        task_id="dbt_test_gold_models",
        python_callable=run_dbt,
        op_args=[["test", "--select", "gold"]],
    )

    dbt_run >> dbt_test
