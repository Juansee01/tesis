"""
DAG: dag_validate
Runs Great Expectations checkpoints on Silver and Gold tables.
Sends alert if any expectation fails.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from fabric_utils import get_storage_options, get_warehouse_connection

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
        # f1_lakehouse is schema-enabled: managed tables live under Tables/dbo/, not Tables/
        path = f"{base}/Tables/dbo/{table_name}"
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
    """
    Gold marts live in the Fabric Warehouse (dbt writes T-SQL there); they are not
    reachable via ABFSS/Delta like the Lakehouse, so we validate them over the SQL
    endpoint: each mart must be non-empty, and mart_pitstop_features (the ML feature
    store) must have no nulls in its key columns and all three label classes present.
    """
    checks_passed = True
    results = []

    # dbt writes the Gold models under schema `dbo` + `+schema: gold` = `dbo_gold`.
    schema = os.environ.get("FABRIC_GOLD_SCHEMA", "dbo_gold")

    marts = [
        "mart_lap_performance",
        "mart_pitstop_strategy",
        "mart_constructor_standings",
        "mart_pitstop_features",
    ]

    conn = get_warehouse_connection()
    try:
        cur = conn.cursor()

        for mart in marts:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{mart}")
            n = cur.fetchone()[0]
            if n == 0:
                results.append(f"FAIL {mart}: 0 rows")
                checks_passed = False
            else:
                results.append(f"OK   {mart}: {n} rows")

        # feature store integrity: no nulls in key/label, all 3 classes present
        cur.execute(
            f"SELECT COUNT(*) FROM {schema}.mart_pitstop_features "
            "WHERE driver_id IS NULL OR pit_window_class IS NULL"
        )
        null_keys = cur.fetchone()[0]
        if null_keys > 0:
            results.append(f"FAIL mart_pitstop_features: {null_keys} rows with null driver_id/pit_window_class")
            checks_passed = False
        else:
            results.append("OK   mart_pitstop_features: no null key/label")

        cur.execute(f"SELECT COUNT(DISTINCT pit_window_class) FROM {schema}.mart_pitstop_features")
        n_classes = cur.fetchone()[0]
        if n_classes < 3:
            results.append(f"FAIL mart_pitstop_features: only {n_classes}/3 label classes present")
            checks_passed = False
        else:
            results.append("OK   mart_pitstop_features: 3 label classes present")
    finally:
        conn.close()

    for r in results:
        print(r)

    if not checks_passed:
        raise ValueError("Gold data quality validation failed — see logs above")


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
