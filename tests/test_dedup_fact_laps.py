"""
Tests for deduplication logic on fact_laps.
The key is (year, round, driver_id, lap_number).
Duplicates can appear if fastf1 returns a lap twice (rare but happens with SC laps).
"""

import pandas as pd
import pytest


def deduplicate_laps(df: pd.DataFrame) -> pd.DataFrame:
    KEY = ["year", "round", "driver_id", "lap_number"]
    # keep the row with the smallest lap_time when duplicates exist (most accurate reading)
    return (
        df.sort_values("lap_time", na_position="last")
        .drop_duplicates(subset=KEY, keep="first")
        .reset_index(drop=True)
    )


def test_no_duplicates_after_dedup():
    df = pd.DataFrame({
        "year":       [2023, 2023, 2023],
        "round":      [5, 5, 5],
        "driver_id":  ["hamilton", "hamilton", "verstappen"],
        "lap_number": [1, 1, 1],
        "lap_time":   [83.1, 83.2, 82.5],
    })
    result = deduplicate_laps(df)
    key = ["year", "round", "driver_id", "lap_number"]
    assert result.duplicated(subset=key).sum() == 0


def test_keeps_lower_lap_time_on_duplicate():
    df = pd.DataFrame({
        "year":       [2023, 2023],
        "round":      [5, 5],
        "driver_id":  ["hamilton", "hamilton"],
        "lap_number": [10, 10],
        "lap_time":   [84.5, 83.1],
    })
    result = deduplicate_laps(df)
    assert len(result) == 1
    assert abs(result.loc[0, "lap_time"] - 83.1) < 0.001


def test_unique_rows_not_dropped():
    df = pd.DataFrame({
        "year":       [2023, 2023, 2023],
        "round":      [5, 5, 5],
        "driver_id":  ["hamilton", "verstappen", "leclerc"],
        "lap_number": [1, 1, 1],
        "lap_time":   [83.1, 82.5, 84.2],
    })
    result = deduplicate_laps(df)
    assert len(result) == 3


def test_empty_dataframe_stays_empty():
    df = pd.DataFrame(columns=["year", "round", "driver_id", "lap_number", "lap_time"])
    result = deduplicate_laps(df)
    assert len(result) == 0
