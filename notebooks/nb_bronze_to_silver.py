"""
Fabric Notebook: Bronze → Silver transformation
Paste this code into a new Fabric Notebook in your workspace.
The notebook reads Delta tables from Bronze and writes cleaned Silver tables.
'spark' is pre-configured in Fabric — no need to create a SparkSession.
"""

# ── Parameters (set via Airflow or Fabric notebook parameters cell) ──────────
# In Fabric, parameters are injected as variables. Defaults here for manual runs.
execution_date = execution_date if "execution_date" in dir() else "2024-01-01"  # noqa

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, StringType
from pyspark.sql.window import Window

LAKEHOUSE = "f1_lakehouse"  # your Fabric Lakehouse name attached to this notebook


# ────────────────────────────────────────────────────────────────────────────
# 1. DRIVERS — unify driver IDs between fastf1 (abbreviation) and Ergast (slug)
# ────────────────────────────────────────────────────────────────────────────

drivers_raw = spark.read.format("delta").load(f"Tables/bronze_drivers")

drivers = (
    drivers_raw
    .dropDuplicates(["driver_id", "year"])
    .withColumn("abbreviation", F.upper(F.col("abbreviation")))
    .withColumn("driver_id", F.lower(F.col("driver_id")))
    # keep latest year's record for each driver_id
    .withColumn("_rank", F.row_number().over(
        Window.partitionBy("driver_id").orderBy(F.desc("year"))
    ))
    .filter(F.col("_rank") == 1)
    .drop("_rank", "year")
)

drivers.write.format("delta").mode("overwrite").saveAsTable(f"{LAKEHOUSE}.silver_dim_drivers")
print(f"dim_drivers: {drivers.count()} drivers")


# ────────────────────────────────────────────────────────────────────────────
# 2. CONSTRUCTORS
# ────────────────────────────────────────────────────────────────────────────

constructors_raw = spark.read.format("delta").load("Tables/bronze_constructors")

constructors = (
    constructors_raw
    .dropDuplicates(["constructor_id"])
    .withColumn("constructor_id", F.lower(F.col("constructor_id")))
)

constructors.write.format("delta").mode("overwrite").saveAsTable(f"{LAKEHOUSE}.silver_dim_constructors")
print(f"dim_constructors: {constructors.count()} constructors")


# ────────────────────────────────────────────────────────────────────────────
# 3. CIRCUITS
# ────────────────────────────────────────────────────────────────────────────

circuits_raw = spark.read.format("delta").load("Tables/bronze_circuits")

circuits = circuits_raw.dropDuplicates(["circuit_id"])

circuits.write.format("delta").mode("overwrite").saveAsTable(f"{LAKEHOUSE}.silver_dim_circuits")
print(f"dim_circuits: {circuits.count()} circuits")


# ────────────────────────────────────────────────────────────────────────────
# 4. FACT_LAPS — main transformation
# ────────────────────────────────────────────────────────────────────────────

laps_raw = spark.read.format("delta").load("Tables/bronze_laps")
results_f1_raw = spark.read.format("delta").load("Tables/bronze_results_fastf1")

# join fastf1 abbreviation to Ergast driver_id
abbrev_to_id = drivers.select("driver_id", "abbreviation")

laps = (
    laps_raw
    # cast types
    .withColumn("lap_time",    F.col("lap_time").cast(DoubleType()))
    .withColumn("sector1_time", F.col("sector1_time").cast(DoubleType()))
    .withColumn("sector2_time", F.col("sector2_time").cast(DoubleType()))
    .withColumn("sector3_time", F.col("sector3_time").cast(DoubleType()))
    .withColumn("tyre_life",   F.col("tyre_life").cast(IntegerType()))
    .withColumn("lap_number",  F.col("lap_number").cast(IntegerType()))
    # is_valid_lap: True only if lap_time is not null and is_valid_lap is True
    .withColumn(
        "is_valid_lap",
        F.col("is_valid_lap").cast(BooleanType()) & F.col("lap_time").isNotNull()
    )
    .withColumn("driver_abbreviation", F.upper(F.col("driver_abbreviation")))
    # join to get Ergast driver_id
    .join(abbrev_to_id, on="driver_abbreviation", how="left")
    # deduplicate
    .dropDuplicates(["year", "round", "driver_abbreviation", "lap_number"])
)

# null rate check (log only — Great Expectations handles the hard validation)
null_rate = laps.filter(F.col("lap_time").isNull()).count() / laps.count()
print(f"fact_laps null rate (lap_time): {null_rate:.2%}")

laps.write.format("delta").mode("overwrite").partitionBy("year", "round").saveAsTable(
    f"{LAKEHOUSE}.silver_fact_laps"
)
print(f"fact_laps: {laps.count()} rows")


# ────────────────────────────────────────────────────────────────────────────
# 5. FACT_PITSTOPS
# ────────────────────────────────────────────────────────────────────────────

pitstops_raw = spark.read.format("delta").load("Tables/bronze_pitstops")

pitstops = (
    pitstops_raw
    .withColumn("lap",      F.col("lap").cast(IntegerType()))
    .withColumn("stop",     F.col("stop").cast(IntegerType()))
    .withColumn("duration", F.col("duration").cast(DoubleType()))
    .withColumn("driver_id", F.lower(F.col("driver_id")))
    # outlier filter: pit stop durations < 15s or > 120s are likely data errors
    .filter((F.col("duration").isNull()) | (F.col("duration").between(15, 120)))
    .dropDuplicates(["year", "round", "driver_id", "stop"])
)

pitstops.write.format("delta").mode("overwrite").partitionBy("year", "round").saveAsTable(
    f"{LAKEHOUSE}.silver_fact_pitstops"
)
print(f"fact_pitstops: {pitstops.count()} rows")


# ────────────────────────────────────────────────────────────────────────────
# 6. FACT_RESULTS — merge fastf1 + Ergast
# ────────────────────────────────────────────────────────────────────────────

results_ergast_raw = spark.read.format("delta").load("Tables/bronze_results_ergast")

results_ergast = (
    results_ergast_raw
    .withColumn("driver_id",      F.lower(F.col("driver_id")))
    .withColumn("constructor_id", F.lower(F.col("constructor_id")))
    .withColumn("grid",           F.col("grid").cast(IntegerType()))
    .withColumn("position",       F.col("position").cast(IntegerType()))
    .withColumn("points",         F.col("points").cast(DoubleType()))
    .withColumn("total_laps",     F.col("total_laps").cast(IntegerType()))
    .dropDuplicates(["year", "round", "driver_id"])
)

results_ergast.write.format("delta").mode("overwrite").partitionBy("year", "round").saveAsTable(
    f"{LAKEHOUSE}.silver_fact_results"
)
print(f"fact_results: {results_ergast.count()} rows")


# ────────────────────────────────────────────────────────────────────────────
# 7. FACT_QUALIFYING
# ────────────────────────────────────────────────────────────────────────────

qualifying_raw = spark.read.format("delta").load("Tables/bronze_qualifying")

qualifying = (
    qualifying_raw
    .withColumn("driver_id",      F.lower(F.col("driver_id")))
    .withColumn("constructor_id", F.lower(F.col("constructor_id")))
    .withColumn("position",       F.col("position").cast(IntegerType()))
    .dropDuplicates(["year", "round", "driver_id"])
)

qualifying.write.format("delta").mode("overwrite").partitionBy("year", "round").saveAsTable(
    f"{LAKEHOUSE}.silver_fact_qualifying"
)
print(f"fact_qualifying: {qualifying.count()} rows")

print("Bronze → Silver transformation complete")
