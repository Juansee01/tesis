import fastf1
import pandas as pd
from pathlib import Path


class FastF1Client:
    def __init__(self, cache_path: str = "/tmp/fastf1_cache"):
        Path(cache_path).mkdir(parents=True, exist_ok=True)
        fastf1.Cache.enable_cache(cache_path)

    def _load_session(self, year: int, round_number: int, session_type: str = "R"):
        session = fastf1.get_session(year, round_number, session_type)
        session.load(weather=True, messages=False)
        return session

    def get_laps(self, year: int, round_number: int) -> pd.DataFrame:
        session = self._load_session(year, round_number)
        laps = session.laps.copy().reset_index(drop=True)

        laps["year"] = year
        laps["round"] = round_number

        # timedelta columns → float seconds for storage
        for col in ["LapTime", "Sector1Time", "Sector2Time", "Sector3Time"]:
            if col in laps.columns:
                laps[col] = laps[col].dt.total_seconds()

        laps = laps.rename(columns={
            "Driver": "driver_abbreviation",
            "LapNumber": "lap_number",
            "LapTime": "lap_time",
            "Sector1Time": "sector1_time",
            "Sector2Time": "sector2_time",
            "Sector3Time": "sector3_time",
            "Compound": "compound",
            "TyreLife": "tyre_life",
            "IsAccurate": "is_valid_lap",
            "Team": "team_name",
            "TrackStatus": "track_status",
            "Stint": "stint",
        })

        keep = [
            "year", "round", "driver_abbreviation", "lap_number",
            "lap_time", "sector1_time", "sector2_time", "sector3_time",
            "compound", "tyre_life", "is_valid_lap", "team_name",
            "track_status", "stint",
        ]
        return laps[[c for c in keep if c in laps.columns]]

    def get_results(self, year: int, round_number: int) -> pd.DataFrame:
        session = self._load_session(year, round_number)
        results = session.results.copy().reset_index(drop=True)

        results["year"] = year
        results["round"] = round_number

        results = results.rename(columns={
            "Abbreviation": "driver_abbreviation",
            "DriverId": "driver_id_fastf1",
            "TeamName": "team_name",
            "Position": "position",
            "GridPosition": "grid",
            "Points": "points",
            "Status": "status",
            "ClassifiedPosition": "classified_position",
        })

        keep = [
            "year", "round", "driver_abbreviation", "driver_id_fastf1",
            "team_name", "position", "grid", "points", "status",
            "classified_position",
        ]
        return results[[c for c in keep if c in results.columns]]

    def get_weather(self, year: int, round_number: int) -> pd.DataFrame:
        session = self._load_session(year, round_number)
        if session.weather_data is None or session.weather_data.empty:
            return pd.DataFrame()

        weather = session.weather_data.copy().reset_index(drop=True)
        weather["year"] = year
        weather["round"] = round_number

        # boolean for dry/wet: if Rainfall is True for any row → wet race
        if "Rainfall" in weather.columns:
            weather["is_wet"] = weather["Rainfall"].astype(bool)

        return weather[["year", "round", "is_wet"]].drop_duplicates().head(1)

    def get_schedule(self, year: int) -> pd.DataFrame:
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        schedule = schedule.rename(columns={
            "RoundNumber": "round",
            "EventName": "race_name",
            "Country": "country",
            "Location": "location",
            "EventDate": "race_date",
        })
        schedule["year"] = year

        keep = ["year", "round", "race_name", "country", "location", "race_date"]
        return schedule[[c for c in keep if c in schedule.columns]]
