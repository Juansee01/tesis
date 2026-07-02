"""
Tests for lap time normalization logic in the ingestion library.
fastf1 returns timedeltas; we convert them to float seconds before writing to Bronze.
"""

import pandas as pd
import numpy as np
import pytest
from datetime import timedelta


def normalize_lap_time(td) -> float | None:
    """Convert a timedelta (or NaT) to total seconds as float."""
    if pd.isna(td):
        return None
    return td.total_seconds()


def test_normal_lap_time():
    td = timedelta(minutes=1, seconds=23, milliseconds=456)
    result = normalize_lap_time(td)
    assert abs(result - 83.456) < 0.001


def test_sub_minute_lap():
    td = timedelta(seconds=58, milliseconds=123)
    result = normalize_lap_time(td)
    assert abs(result - 58.123) < 0.001


def test_nat_returns_none():
    result = normalize_lap_time(pd.NaT)
    assert result is None


def test_numpy_nan_returns_none():
    result = normalize_lap_time(float("nan"))
    assert result is None


def test_whole_dataframe_conversion():
    df = pd.DataFrame({
        "LapTime": [
            timedelta(minutes=1, seconds=30),
            pd.NaT,
            timedelta(seconds=65, milliseconds=500),
        ]
    })
    df["lap_time"] = df["LapTime"].apply(normalize_lap_time)

    assert abs(df.loc[0, "lap_time"] - 90.0) < 0.001
    assert df.loc[1, "lap_time"] is None
    assert abs(df.loc[2, "lap_time"] - 65.5) < 0.001
