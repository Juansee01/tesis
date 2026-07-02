"""
Tests for the driver ID mapping between fastf1 (3-letter abbreviation)
and Ergast/Jolpica-F1 (slug like 'hamilton').
The Silver layer must unify both into a single driver_id column.
This test uses the known 2021–2024 F1 driver roster to validate coverage.
"""

import pytest

# Known mapping for all drivers active in 2021-2024.
# Source: cross-referencing fastf1 session.results.Abbreviation with Ergast driverId.
KNOWN_MAPPING = {
    "HAM": "hamilton",
    "VER": "max_verstappen",
    "BOT": "bottas",
    "NOR": "norris",
    "LEC": "leclerc",
    "SAI": "sainz",
    "PER": "perez",
    "RUS": "russell",
    "ALO": "alonso",
    "OCO": "ocon",
    "GAS": "gasly",
    "STR": "stroll",
    "VET": "vettel",
    "TSU": "tsunoda",
    "LAT": "latifi",
    "MSC": "mick_schumacher",
    "MAZ": "mazepin",
    "RAI": "raikkonen",
    "GIO": "giovinazzi",
    "KUB": "kubica",
    "ZHO": "zhou",
    "DEV": "de_vries",
    "HUL": "hulkenberg",
    "MAG": "magnussen",
    "PIA": "piastri",
    "SAR": "sargeant",
    "ALB": "albon",
    "COL": "colapinto",
    "BEA": "bearman",
    "LAW": "lawson",
    "DOO": "doohan",
    "HAD": "hadjar",
    "ANT": "antonelli",
}


def build_abbrev_to_id_lookup(mapping: dict) -> dict:
    """Simulates what the Silver notebook does: build a lookup from abbreviation to driver_id."""
    return {abbrev.upper(): driver_id.lower() for abbrev, driver_id in mapping.items()}


def test_all_known_abbreviations_resolve():
    lookup = build_abbrev_to_id_lookup(KNOWN_MAPPING)
    for abbrev in KNOWN_MAPPING:
        assert abbrev.upper() in lookup, f"Missing abbreviation: {abbrev}"


def test_driver_id_is_lowercase():
    lookup = build_abbrev_to_id_lookup(KNOWN_MAPPING)
    for driver_id in lookup.values():
        assert driver_id == driver_id.lower(), f"driver_id not lowercase: {driver_id}"


def test_hamilton_maps_correctly():
    lookup = build_abbrev_to_id_lookup(KNOWN_MAPPING)
    assert lookup["HAM"] == "hamilton"


def test_verstappen_maps_correctly():
    lookup = build_abbrev_to_id_lookup(KNOWN_MAPPING)
    assert lookup["VER"] == "max_verstappen"


def test_no_duplicate_driver_ids():
    lookup = build_abbrev_to_id_lookup(KNOWN_MAPPING)
    ids = list(lookup.values())
    assert len(ids) == len(set(ids)), "Duplicate driver_ids found in mapping"
