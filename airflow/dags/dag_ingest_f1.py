"""
DAG: dag_ingest_f1
Extracts data from fastf1 and Ergast/Jolpica-F1 and loads it into OneLake Bronze layer.
Runs weekly during the F1 season. For historical backfills use:
    airflow dags backfill dag_ingest_f1 -s 2021-01-01 -e 2024-12-31
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.http_sensor import HttpSensor

from ingestion import FastF1Client, ErgastClient, OneLakeLoader
from fabric_utils import get_storage_options

YEARS = [2021, 2022, 2023, 2024]

default_args = {
    "owner": "juan_lizarralde",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["juanlizarralde@turismocity.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def ingest_grand_prix(year: int, round_number: int, **context):
    storage_options = get_storage_options()
    loader = OneLakeLoader(
        storage_options=storage_options,
        lakehouse_abfss=os.environ["ONELAKE_ABFSS"],
    )

    f1 = FastF1Client(cache_path="/tmp/fastf1_cache")
    ergast = ErgastClient()

    laps = f1.get_laps(year, round_number)
    results_f1 = f1.get_results(year, round_number)
    weather = f1.get_weather(year, round_number)

    results_ergast = ergast.get_race_results(year, round_number)
    pitstops = ergast.get_pitstops(year, round_number)
    qualifying = ergast.get_qualifying(year, round_number)

    loader.write_bronze(laps, "laps", year, round_number)
    loader.write_bronze(results_f1, "results_fastf1", year, round_number)
    loader.write_bronze(results_ergast, "results_ergast", year, round_number)
    loader.write_bronze(pitstops, "pitstops", year, round_number)
    loader.write_bronze(qualifying, "qualifying", year, round_number)
    if not weather.empty:
        loader.write_bronze(weather, "weather", year, round_number)

    print(f"Bronze ingestion complete: {year} R{round_number} — {len(laps)} laps, {len(pitstops)} pit stops")


def ingest_season_dims(year: int, **context):
    storage_options = get_storage_options()
    loader = OneLakeLoader(
        storage_options=storage_options,
        lakehouse_abfss=os.environ["ONELAKE_ABFSS"],
    )

    ergast = ErgastClient()
    f1 = FastF1Client(cache_path="/tmp/fastf1_cache")

    drivers = ergast.get_drivers(year)
    constructors = ergast.get_constructors(year)
    circuits = ergast.get_circuits(year)
    schedule = f1.get_schedule(year)

    loader.write_bronze_dim(drivers, "drivers", year)
    loader.write_bronze_dim(constructors, "constructors", year)
    loader.write_bronze_dim(circuits, "circuits", year)
    loader.write_bronze_dim(schedule, "schedule", year)

    print(f"Dimension ingestion complete: {year} — {len(drivers)} drivers, {len(circuits)} circuits")


with DAG(
    dag_id="dag_ingest_f1",
    default_args=default_args,
    description="Ingest F1 data (fastf1 + Ergast) to Bronze layer in OneLake",
    schedule="0 8 * * MON",  # every Monday at 08:00 UTC (day after race weekend)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "bronze"],
) as dag:

    # Check Jolpica-F1 API is reachable before starting
    check_api = HttpSensor(
        task_id="check_jolpica_api",
        http_conn_id="jolpica_f1",
        endpoint="/ergast/f1/2024.json",
        poke_interval=60,
        timeout=300,
    )

    for year in YEARS:
        ingest_dims = PythonOperator(
            task_id=f"ingest_dims_{year}",
            python_callable=ingest_season_dims,
            op_kwargs={"year": year},
        )

        check_api >> ingest_dims
