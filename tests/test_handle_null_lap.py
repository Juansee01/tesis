"""
Tests for the is_valid_lap flag logic.
A lap without a recorded time (pit in/out, abandonment, red flag) must be
flagged as invalid rather than dropped, to preserve lineage in Silver.
"""

import pandas as pd


def assign_valid_lap_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    A lap is valid only when:
    - lap_time is not null
    - is_accurate (from fastf1) is True
    """
    df = df.copy()
    has_time = df["lap_time"].notna()
    is_accurate = df["is_accurate"].fillna(False).astype(bool)
    df["is_valid_lap"] = has_time & is_accurate
    return df


def make_laps(**kwargs) -> pd.DataFrame:
    defaults = {
        "lap_number": [1, 2, 3, 4, 5],
        "lap_time": [83.1, None, 84.2, None, 85.0],
        "is_accurate": [True, False, True, True, True],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


def test_null_lap_time_flagged_invalid():
    df = assign_valid_lap_flag(make_laps())
    assert not df.loc[1, "is_valid_lap"]


def test_null_lap_time_not_dropped():
    df = assign_valid_lap_flag(make_laps())
    assert len(df) == 5  # rows preserved


def test_accurate_flag_false_marks_invalid():
    df = assign_valid_lap_flag(make_laps())
    # vuelta 1 (indice 0): time=83.1, is_accurate=True -> valida
    assert df.loc[0, "is_valid_lap"]
    # vuelta 2 (indice 1): time=None -> invalida pase lo que pase con is_accurate
    assert not df.loc[1, "is_valid_lap"]


def test_time_present_but_inaccurate():
    df = pd.DataFrame(
        {
            "lap_number": [1],
            "lap_time": [83.5],
            "is_accurate": [False],
        }
    )
    result = assign_valid_lap_flag(df)
    # tiene tiempo pero fastf1 la marco como inaccurate, tiene que dar invalida
    assert not result.loc[0, "is_valid_lap"]


def test_all_valid_laps():
    df = pd.DataFrame(
        {
            "lap_number": [1, 2, 3],
            "lap_time": [80.1, 80.2, 80.3],
            "is_accurate": [True, True, True],
        }
    )
    result = assign_valid_lap_flag(df)
    assert result["is_valid_lap"].all()
