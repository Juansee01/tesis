import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

BASE_URL = "https://api.jolpi.ca/ergast/f1"


class ErgastClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout)),
    )
    def _get(self, endpoint: str) -> dict:
        url = f"{BASE_URL}{endpoint}.json"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_race_results(self, year: int, round_number: int) -> pd.DataFrame:
        data = self._get(f"/{year}/{round_number}/results")
        races = data["MRData"]["RaceTable"].get("Races", [])
        if not races:
            return pd.DataFrame()

        rows = []
        for r in races[0].get("Results", []):
            rows.append({
                "year": year,
                "round": round_number,
                "driver_id": r["Driver"]["driverId"],
                "constructor_id": r["Constructor"]["constructorId"],
                "grid": int(r.get("grid", 0)),
                "position": int(r["position"]) if r["position"].isdigit() else None,
                "points": float(r.get("points", 0)),
                "status": r.get("status"),
                "total_laps": int(r.get("laps", 0)),
            })
        return pd.DataFrame(rows)

    def get_pitstops(self, year: int, round_number: int) -> pd.DataFrame:
        data = self._get(f"/{year}/{round_number}/pitstops")
        races = data["MRData"]["RaceTable"].get("Races", [])
        if not races:
            return pd.DataFrame()

        rows = []
        for p in races[0].get("PitStops", []):
            duration_raw = p.get("duration", "")
            try:
                duration = float(duration_raw)
            except (ValueError, TypeError):
                duration = None

            rows.append({
                "year": year,
                "round": round_number,
                "driver_id": p["driverId"],
                "stop": int(p["stop"]),
                "lap": int(p["lap"]),
                "duration": duration,
            })
        return pd.DataFrame(rows)

    def get_qualifying(self, year: int, round_number: int) -> pd.DataFrame:
        data = self._get(f"/{year}/{round_number}/qualifying")
        races = data["MRData"]["RaceTable"].get("Races", [])
        if not races:
            return pd.DataFrame()

        rows = []
        for q in races[0].get("QualifyingResults", []):
            rows.append({
                "year": year,
                "round": round_number,
                "driver_id": q["Driver"]["driverId"],
                "constructor_id": q["Constructor"]["constructorId"],
                "position": int(q["position"]),
                "q1": q.get("Q1"),
                "q2": q.get("Q2"),
                "q3": q.get("Q3"),
            })
        return pd.DataFrame(rows)

    def get_drivers(self, year: int) -> pd.DataFrame:
        data = self._get(f"/{year}/drivers")
        rows = []
        for d in data["MRData"]["DriverTable"].get("Drivers", []):
            rows.append({
                "driver_id": d["driverId"],
                "abbreviation": d.get("code", ""),
                "forename": d["givenName"],
                "surname": d["familyName"],
                "nationality": d.get("nationality"),
                "date_of_birth": d.get("dateOfBirth"),
            })
        return pd.DataFrame(rows)

    def get_constructors(self, year: int) -> pd.DataFrame:
        data = self._get(f"/{year}/constructors")
        rows = []
        for c in data["MRData"]["ConstructorTable"].get("Constructors", []):
            rows.append({
                "constructor_id": c["constructorId"],
                "constructor_name": c["name"],
                "nationality": c.get("nationality"),
            })
        return pd.DataFrame(rows)

    def get_circuits(self, year: int) -> pd.DataFrame:
        data = self._get(f"/{year}/circuits")
        rows = []
        for c in data["MRData"]["CircuitTable"].get("Circuits", []):
            rows.append({
                "circuit_id": c["circuitId"],
                "circuit_name": c["circuitName"],
                "country": c["Location"]["country"],
                "location": c["Location"]["locality"],
            })
        return pd.DataFrame(rows)

    def get_schedule(self, year: int) -> pd.DataFrame:
        data = self._get(f"/{year}")
        rows = []
        for r in data["MRData"]["RaceTable"].get("Races", []):
            rows.append({
                "year": year,
                "round": int(r["round"]),
                "race_name": r["raceName"],
                "circuit_id": r["Circuit"]["circuitId"],
                "race_date": r.get("date"),
            })
        return pd.DataFrame(rows)
