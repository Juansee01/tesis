"""
DAG: dag_ingest_f1
Extracts data from fastf1 and Ergast/Jolpica-F1 and loads it into OneLake Bronze layer.
For the historical backfill (2021-2024) trigger manually from the Airflow UI.
Weekly schedule picks up the most recent GP after each race weekend.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion import FastF1Client, ErgastClient, OneLakeLoader
from fabric_utils import get_storage_options

# rounds per season — used to generate tasks for historical backfill
SEASON_ROUNDS = {
    2021: 22,
    2022: 22,
    2023: 23,
    2024: 24,
}

default_args = {
    "owner": "juan_lizarralde",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


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

    print(f"Dims done: {year} — {len(drivers)} drivers, {len(circuits)} circuits")


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

    print(f"GP done: {year} R{round_number} — {len(laps)} laps, {len(pitstops)} pit stops")


with DAG(
    dag_id="dag_ingest_f1",
    default_args=default_args,
    description="Ingest F1 data (fastf1 + Ergast) to Bronze — historical 2021-2024",
    schedule=None,  # manual trigger; set to '0 8 * * MON' for weekly
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["ingestion", "bronze"],
) as dag:

    for year, total_rounds in SEASON_ROUNDS.items():
        # dimension task per season
        dim_task = PythonOperator(
            task_id=f"ingest_dims_{year}",
            python_callable=ingest_season_dims,
            op_kwargs={"year": year},
        )

        prev_task = dim_task

        # one task per GP in the season
        for round_num in range(1, total_rounds + 1):
            gp_task = PythonOperator(
                task_id=f"ingest_gp_{year}_r{round_num:02d}",
                python_callable=ingest_grand_prix,
                op_kwargs={"year": year, "round_number": round_num},
            )
            prev_task >> gp_task
            prev_task = gp_task
