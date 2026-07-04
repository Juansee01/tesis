"""
Fabric Notebook: Bronze → Silver transformation
Paste this code into the Fabric Notebook 'nb_bronze_to_silver' (replace existing content).
Each Bronze table was written as a separate Delta table per year/round, so we union them here.
"""

from functools import reduce
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from pyspark.sql.window import Window

SEASON_ROUNDS = {2021: 22, 2022: 22, 2023: 22, 2024: 24}
YEARS = sorted(SEASON_ROUNDS.keys())


def _load_dim(table: str) -> DataFrame:
    dfs = []
    for y in YEARS:
        try:
            df = (
                spark.read.format("delta")
                .load(f"Tables/bronze_{table}/year={y}")
                .withColumn("year", F.lit(y).cast(IntegerType()))
            )
            dfs.append(df)
        except Exception as e:
            print(f"  skip bronze_{table}/year={y}: {e}")
    if not dfs:
        raise RuntimeError(f"No Bronze data found for table: {table}")
    return reduce(DataFrame.union, dfs)


def _load_fact(table: str) -> DataFrame:
    dfs = []
    for y, max_r in SEASON_ROUNDS.items():
        for r in range(1, max_r + 1):
            try:
                df = (
                    spark.read.format("delta")
                    .load(f"Tables/bronze_{table}/year={y}/round={r}")
                    .withColumn("year", F.lit(y).cast(IntegerType()))
                    .withColumn("round", F.lit(r).cast(IntegerType()))
                )
                dfs.append(df)
            except Exception:
                pass
    if not dfs:
        raise RuntimeError(f"No Bronze data found for table: {table}")
    return reduce(DataFrame.union, dfs)


# ── 1. DRIVERS ────────────────────────────────────────────────────────────────

drivers_raw = _load_dim("drivers")

drivers = (
    drivers_raw
    .dropDuplicates(["driver_id", "year"])
    .withColumn("abbreviation", F.upper(F.col("abbreviation")))
    .withColumn("driver_id", F.lower(F.col("driver_id")))
    .withColumn("_rank", F.row_number().over(
        Window.partitionBy("driver_id").orderBy(F.desc("year"))
    ))
    .filter(F.col("_rank") == 1)
    .drop("_rank", "year")
)

drivers.write.format("delta").mode("overwrite").saveAsTable("silver_dim_drivers")
print(f"silver_dim_drivers: {drivers.count()} rows")


# ── 2. CONSTRUCTORS ───────────────────────────────────────────────────────────

constructors_raw = _load_dim("constructors")

constructors = (
    constructors_raw
    .dropDuplicates(["constructor_id"])
    .withColumn("constructor_id", F.lower(F.col("constructor_id")))
)

constructors.write.format("delta").mode("overwrite").saveAsTable("silver_dim_constructors")
print(f"silver_dim_constructors: {constructors.count()} rows")


# ── 3. CIRCUITS ───────────────────────────────────────────────────────────────

circuits_raw = _load_dim("circuits")

circuits = circuits_raw.dropDuplicates(["circuit_id"])

circuits.write.format("delta").mode("overwrite").saveAsTable("silver_dim_circuits")
print(f"silver_dim_circuits: {circuits.count()} rows")


# ── 4. FACT_LAPS ──────────────────────────────────────────────────────────────

laps_raw = _load_fact("laps")

abbrev_to_id = drivers.select(
    F.col("driver_id"),
    F.col("abbreviation").alias("driver_abbreviation"),
)

laps = (
    laps_raw
    .withColumn("lap_time",     F.col("lap_time").cast(DoubleType()))
    .withColumn("sector1_time", F.col("sector1_time").cast(DoubleType()))
    .withColumn("sector2_time", F.col("sector2_time").cast(DoubleType()))
    .withColumn("sector3_time", F.col("sector3_time").cast(DoubleType()))
    .withColumn("tyre_life",    F.col("tyre_life").cast(IntegerType()))
    .withColumn("lap_number",   F.col("lap_number").cast(IntegerType()))
    .withColumn(
        "is_valid_lap",
        F.col("is_valid_lap").cast(BooleanType()) & F.col("lap_time").isNotNull()
    )
    .withColumn("driver_abbreviation", F.upper(F.col("driver_abbreviation")))
    .join(abbrev_to_id, on="driver_abbreviation", how="left")
    .dropDuplicates(["year", "round", "driver_abbreviation", "lap_number"])
)

null_rate = laps.filter(F.col("lap_time").isNull()).count() / laps.count()
print(f"silver_fact_laps null rate (lap_time): {null_rate:.2%}")

laps.write.format("delta").mode("overwrite").partitionBy("year", "round").saveAsTable(
    "silver_fact_laps"
)
print(f"silver_fact_laps: {laps.count()} rows")


# ── 5. FACT_PITSTOPS ──────────────────────────────────────────────────────────

pitstops_raw = _load_fact("pitstops")

pitstops = (
    pitstops_raw
    .withColumn("lap",      F.col("lap").cast(IntegerType()))
    .withColumn("stop",     F.col("stop").cast(IntegerType()))
    .withColumn("duration", F.col("duration").cast(DoubleType()))
    .withColumn("driver_id", F.lower(F.col("driver_id")))
    .filter((F.col("duration").isNull()) | (F.col("duration").between(15, 120)))
    .dropDuplicates(["year", "round", "driver_id", "stop"])
)

pitstops.write.format("delta").mode("overwrite").partitionBy("year", "round").saveAsTable(
    "silver_fact_pitstops"
)
print(f"silver_fact_pitstops: {pitstops.count()} rows")


# ── 6. FACT_RESULTS ───────────────────────────────────────────────────────────

results_ergast_raw = _load_fact("results_ergast")

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
    "silver_fact_results"
)
print(f"silver_fact_results: {results_ergast.count()} rows")


# ── 7. FACT_QUALIFYING ────────────────────────────────────────────────────────

qualifying_raw = _load_fact("qualifying")

qualifying = (
    qualifying_raw
    .withColumn("driver_id",      F.lower(F.col("driver_id")))
    .withColumn("constructor_id", F.lower(F.col("constructor_id")))
    .withColumn("position",       F.col("position").cast(IntegerType()))
    .dropDuplicates(["year", "round", "driver_id"])
)

qualifying.write.format("delta").mode("overwrite").partitionBy("year", "round").saveAsTable(
    "silver_fact_qualifying"
)
print(f"silver_fact_qualifying: {qualifying.count()} rows")


print("\nBronze → Silver transformation complete.")
