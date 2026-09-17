"""
Tests reales sobre ErgastClient: mockeamos _get (la llamada HTTP) y verificamos
que cada metodo parsea bien la respuesta de la API (Jolpica-F1/Ergast), incluido
el caso de carrera sin datos (Races vacio -> DataFrame vacio).
"""

from unittest.mock import patch

from ingestion.ergast_client import ErgastClient


def make_client():
    return ErgastClient()


@patch.object(ErgastClient, "_get")
def test_get_race_results(mock_get):
    mock_get.return_value = {
        "MRData": {
            "RaceTable": {
                "Races": [
                    {
                        "Results": [
                            {
                                "Driver": {"driverId": "hamilton"},
                                "Constructor": {"constructorId": "mercedes"},
                                "grid": "1",
                                "position": "1",
                                "points": "25",
                                "status": "Finished",
                                "laps": "58",
                            }
                        ]
                    }
                ]
            }
        }
    }
    df = make_client().get_race_results(2023, 5)
    assert len(df) == 1
    assert df.loc[0, "driver_id"] == "hamilton"
    assert df.loc[0, "position"] == 1
    assert df.loc[0, "points"] == 25.0


@patch.object(ErgastClient, "_get")
def test_get_race_results_non_numeric_position(mock_get):
    mock_get.return_value = {
        "MRData": {
            "RaceTable": {
                "Races": [
                    {
                        "Results": [
                            {
                                "Driver": {"driverId": "sainz"},
                                "Constructor": {"constructorId": "ferrari"},
                                "grid": "3",
                                "position": "R",
                                "points": "0",
                                "status": "Retired",
                                "laps": "12",
                            }
                        ]
                    }
                ]
            }
        }
    }
    df = make_client().get_race_results(2023, 5)
    assert df.loc[0, "position"] is None


@patch.object(ErgastClient, "_get")
def test_get_race_results_no_races_returns_empty(mock_get):
    mock_get.return_value = {"MRData": {"RaceTable": {"Races": []}}}
    df = make_client().get_race_results(2023, 5)
    assert df.empty


@patch.object(ErgastClient, "_get")
def test_get_pitstops(mock_get):
    mock_get.return_value = {
        "MRData": {
            "RaceTable": {
                "Races": [
                    {
                        "PitStops": [
                            {"driverId": "verstappen", "stop": "1", "lap": "20", "duration": "22.5"}
                        ]
                    }
                ]
            }
        }
    }
    df = make_client().get_pitstops(2023, 5)
    assert len(df) == 1
    assert df.loc[0, "duration"] == 22.5


@patch.object(ErgastClient, "_get")
def test_get_pitstops_invalid_duration_is_none(mock_get):
    mock_get.return_value = {
        "MRData": {
            "RaceTable": {
                "Races": [
                    {"PitStops": [{"driverId": "norris", "stop": "1", "lap": "10", "duration": ""}]}
                ]
            }
        }
    }
    df = make_client().get_pitstops(2023, 5)
    assert df.loc[0, "duration"] is None


@patch.object(ErgastClient, "_get")
def test_get_pitstops_no_races_returns_empty(mock_get):
    mock_get.return_value = {"MRData": {"RaceTable": {"Races": []}}}
    df = make_client().get_pitstops(2023, 5)
    assert df.empty


@patch.object(ErgastClient, "_get")
def test_get_qualifying(mock_get):
    mock_get.return_value = {
        "MRData": {
            "RaceTable": {
                "Races": [
                    {
                        "QualifyingResults": [
                            {
                                "Driver": {"driverId": "leclerc"},
                                "Constructor": {"constructorId": "ferrari"},
                                "position": "1",
                                "Q1": "1:20.1",
                                "Q2": "1:19.5",
                                "Q3": "1:19.0",
                            }
                        ]
                    }
                ]
            }
        }
    }
    df = make_client().get_qualifying(2023, 5)
    assert len(df) == 1
    assert df.loc[0, "q3"] == "1:19.0"


@patch.object(ErgastClient, "_get")
def test_get_qualifying_no_races_returns_empty(mock_get):
    mock_get.return_value = {"MRData": {"RaceTable": {"Races": []}}}
    df = make_client().get_qualifying(2023, 5)
    assert df.empty


@patch.object(ErgastClient, "_get")
def test_get_drivers(mock_get):
    mock_get.return_value = {
        "MRData": {
            "DriverTable": {
                "Drivers": [
                    {
                        "driverId": "hamilton",
                        "code": "HAM",
                        "givenName": "Lewis",
                        "familyName": "Hamilton",
                        "nationality": "British",
                        "dateOfBirth": "1985-01-07",
                    }
                ]
            }
        }
    }
    df = make_client().get_drivers(2023)
    assert df.loc[0, "abbreviation"] == "HAM"


@patch.object(ErgastClient, "_get")
def test_get_constructors(mock_get):
    mock_get.return_value = {
        "MRData": {
            "ConstructorTable": {
                "Constructors": [
                    {"constructorId": "mercedes", "name": "Mercedes", "nationality": "German"}
                ]
            }
        }
    }
    df = make_client().get_constructors(2023)
    assert df.loc[0, "constructor_name"] == "Mercedes"


@patch.object(ErgastClient, "_get")
def test_get_circuits(mock_get):
    mock_get.return_value = {
        "MRData": {
            "CircuitTable": {
                "Circuits": [
                    {
                        "circuitId": "monza",
                        "circuitName": "Autodromo di Monza",
                        "Location": {"country": "Italy", "locality": "Monza"},
                    }
                ]
            }
        }
    }
    df = make_client().get_circuits(2023)
    assert df.loc[0, "country"] == "Italy"


@patch.object(ErgastClient, "_get")
def test_get_schedule(mock_get):
    mock_get.return_value = {
        "MRData": {
            "RaceTable": {
                "Races": [
                    {
                        "round": "1",
                        "raceName": "Bahrain Grand Prix",
                        "Circuit": {"circuitId": "bahrain"},
                        "date": "2023-03-05",
                    }
                ]
            }
        }
    }
    df = make_client().get_schedule(2023)
    assert df.loc[0, "race_name"] == "Bahrain Grand Prix"
    assert df.loc[0, "round"] == 1
