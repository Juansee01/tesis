"""
DAG: dag_validate
Runs Great Expectations checkpoints on Silver and Gold tables.
Sends alert if any expectation fails.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from fabric_utils import get_storage_options

default_args = {
    "owner": "juan_lizarralde",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["juanlizarralde@turismocity.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def validate_silver_tables(**context):
    """
    Reads Silver Delta tables and runs quality checks.
    I'm not using the full GE CLI here because running it against OneLake
    from Airflow requires a custom DataConnector config - keeping it simple
    with direct DataFrame checks for now.
    """
    from deltalake import DeltaTable
    import pandas as pd

    storage_options = get_storage_options()
    base = os.environ["ONELAKE_ABFSS"]

    checks_passed = True
    results = []

    tables_to_check = {
        "silver_fact_laps": ["driver_abbreviation", "lap_number", "year", "round"],
        "silver_fact_pitstops": ["driver_id", "lap", "year", "round"],
        "silver_fact_results": ["driver_id", "position", "year", "round"],
    }

    for table_name, key_cols in tables_to_check.items():
        path = f"{base}/Tables/{table_name}"
        dt = DeltaTable(path, storage_options=storage_options)
        df = dt.to_pandas()

        for col in key_cols:
            if col not in df.columns:
                results.append(f"FAIL {table_name}: missing column '{col}'")
                checks_passed = False
                continue

            null_rate = df[col].isna().mean()
            if null_rate > 0.02:
                results.append(f"FAIL {table_name}.{col}: null rate {null_rate:.2%} > 2%")
                checks_passed = False
            else:
                results.append(f"OK   {table_name}.{col}: null rate {null_rate:.2%}")

        # deduplication check on fact_laps
        if table_name == "silver_fact_laps":
            dupe_cols = ["year", "round", "driver_abbreviation", "lap_number"]
            if all(c in df.columns for c in dupe_cols):
                dupes = df.duplicated(subset=dupe_cols).sum()
                if dupes > 0:
                    results.append(f"FAIL silver_fact_laps: {dupes} duplicate rows on key")
                    checks_passed = False
                else:
                    results.append("OK   silver_fact_laps: no duplicates on key")

    for r in results:
        print(r)

    if not checks_passed:
        raise ValueError("Data quality validation failed — see logs above")


def validate_gold_tables(**context):
    from deltalake import DeltaTable

    storage_options = get_storage_options()
    base = os.environ["ONELAKE_ABFSS"]

    gold_tables = [
        "gold_mart_lap_performance",
        "gold_mart_pitstop_strategy",
        "gold_mart_constructor_standings",
        "gold_mart_pitstop_features",
    ]

    for table_name in gold_tables:
        path = f"{base}/Tables/{table_name}"
        dt = DeltaTable(path, storage_options=storage_options)
        df = dt.to_pandas()
        print(f"OK   {table_name}: {len(df)} rows")


with DAG(
    dag_id="dag_validate",
    default_args=default_args,
    description="Run data quality checks on Silver and Gold tables",
    schedule="0 12 * * MON",  # after gold DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["validation", "quality"],
) as dag:

    validate_silver = PythonOperator(
        task_id="validate_silver_tables",
        python_callable=validate_silver_tables,
    )

    validate_gold = PythonOperator(
        task_id="validate_gold_tables",
        python_callable=validate_gold_tables,
    )

    validate_silver >> validate_gold
