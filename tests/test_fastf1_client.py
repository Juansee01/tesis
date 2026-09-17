"""
Tests reales sobre FastF1Client: mockeamos fastf1 (Cache, get_session,
get_event_schedule) para no depender de la red ni de la cache local, y
verificamos el renombrado/seleccion de columnas de cada metodo.
"""

from unittest.mock import patch, MagicMock
import pandas as pd

from ingestion.fastf1_client import FastF1Client


def make_client(tmp_path):
    with patch("ingestion.fastf1_client.fastf1.Cache.enable_cache"):
        return FastF1Client(cache_path=str(tmp_path))


@patch("ingestion.fastf1_client.fastf1.get_session")
def test_get_laps_renames_and_converts_timedelta(mock_get_session, tmp_path):
    laps_df = pd.DataFrame(
        {
            "Driver": ["HAM"],
            "LapNumber": [1],
            "LapTime": [pd.Timedelta(seconds=83.1)],
            "Sector1Time": [pd.Timedelta(seconds=27.0)],
            "Sector2Time": [pd.Timedelta(seconds=28.0)],
            "Sector3Time": [pd.Timedelta(seconds=28.1)],
            "Compound": ["SOFT"],
            "TyreLife": [3],
            "IsAccurate": [True],
            "Team": ["Mercedes"],
            "TrackStatus": ["1"],
            "Stint": [1],
        }
    )
    session = MagicMock()
    session.laps = laps_df
    mock_get_session.return_value = session

    client = make_client(tmp_path)
    result = client.get_laps(2023, 5)

    assert result.loc[0, "driver_abbreviation"] == "HAM"
    assert result.loc[0, "lap_time"] == 83.1
    assert result.loc[0, "year"] == 2023
    assert result.loc[0, "round"] == 5
    assert "compound" in result.columns


@patch("ingestion.fastf1_client.fastf1.get_session")
def test_get_results_renames_columns(mock_get_session, tmp_path):
    results_df = pd.DataFrame(
        {
            "Abbreviation": ["VER"],
            "DriverId": ["max_verstappen"],
            "TeamName": ["Red Bull"],
            "Position": [1],
            "GridPosition": [1],
            "Points": [25],
            "Status": ["Finished"],
            "ClassifiedPosition": ["1"],
        }
    )
    session = MagicMock()
    session.results = results_df
    mock_get_session.return_value = session

    client = make_client(tmp_path)
    result = client.get_results(2023, 5)

    assert result.loc[0, "driver_abbreviation"] == "VER"
    assert result.loc[0, "team_name"] == "Red Bull"


@patch("ingestion.fastf1_client.fastf1.get_session")
def test_get_weather_is_wet_true(mock_get_session, tmp_path):
    weather_df = pd.DataFrame({"Rainfall": [False, True, False]})
    session = MagicMock()
    session.weather_data = weather_df
    mock_get_session.return_value = session

    client = make_client(tmp_path)
    result = client.get_weather(2021, 12)

    assert len(result) == 1
    assert bool(result.loc[0, "is_wet"]) is True


@patch("ingestion.fastf1_client.fastf1.get_session")
def test_get_weather_empty_returns_empty_dataframe(mock_get_session, tmp_path):
    session = MagicMock()
    session.weather_data = pd.DataFrame()
    mock_get_session.return_value = session

    client = make_client(tmp_path)
    result = client.get_weather(2021, 12)

    assert result.empty


@patch("ingestion.fastf1_client.fastf1.get_event_schedule")
def test_get_schedule_renames_columns(mock_get_schedule, tmp_path):
    schedule_df = pd.DataFrame(
        {
            "RoundNumber": [1],
            "EventName": ["Bahrain Grand Prix"],
            "Country": ["Bahrain"],
            "Location": ["Sakhir"],
            "EventDate": ["2023-03-05"],
        }
    )
    mock_get_schedule.return_value = schedule_df

    client = make_client(tmp_path)
    result = client.get_schedule(2023)

    assert result.loc[0, "race_name"] == "Bahrain Grand Prix"
    assert result.loc[0, "year"] == 2023
