"""
Tests reales sobre OneLakeLoader: mockeamos write_deltalake para no depender
de OneLake/Azure, y verificamos el path armado por capa/tabla, el guard de
dataframe vacio, y que schema_mode solo se fuerza a "overwrite" en overwrite.
"""

from unittest.mock import patch
import pandas as pd

from ingestion.loader import OneLakeLoader

STORAGE_OPTIONS = {"account_name": "onelake", "bearer_token": "fake"}
BASE = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh"


def make_loader():
    return OneLakeLoader(storage_options=STORAGE_OPTIONS, lakehouse_abfss=BASE + "/")


@patch("ingestion.loader.write_deltalake")
def test_write_skips_empty_dataframe(mock_write):
    make_loader()._write(pd.DataFrame(), "some/path")
    mock_write.assert_not_called()


@patch("ingestion.loader.write_deltalake")
def test_write_skips_none(mock_write):
    make_loader()._write(None, "some/path")
    mock_write.assert_not_called()


@patch("ingestion.loader.write_deltalake")
def test_write_overwrite_sets_schema_mode(mock_write):
    df = pd.DataFrame({"a": [1]})
    make_loader()._write(df, "some/path", mode="overwrite")
    _, kwargs = mock_write.call_args
    assert kwargs["schema_mode"] == "overwrite"
    assert kwargs["mode"] == "overwrite"


@patch("ingestion.loader.write_deltalake")
def test_write_append_does_not_set_schema_mode(mock_write):
    df = pd.DataFrame({"a": [1]})
    make_loader()._write(df, "some/path", mode="append")
    _, kwargs = mock_write.call_args
    assert kwargs["schema_mode"] is None
    assert kwargs["mode"] == "append"


@patch("ingestion.loader.write_deltalake")
def test_write_bronze_builds_partitioned_path(mock_write):
    df = pd.DataFrame({"a": [1]})
    make_loader().write_bronze(df, "fact_laps", year=2023, round_number=5)
    args, _ = mock_write.call_args
    assert args[0] == f"{BASE}/Tables/bronze_fact_laps/year=2023/round=5"


@patch("ingestion.loader.write_deltalake")
def test_write_bronze_dim_builds_year_only_path(mock_write):
    df = pd.DataFrame({"a": [1]})
    make_loader().write_bronze_dim(df, "dim_drivers", year=2023)
    args, _ = mock_write.call_args
    assert args[0] == f"{BASE}/Tables/bronze_dim_drivers/year=2023"


@patch("ingestion.loader.write_deltalake")
def test_write_table_builds_layer_table_path(mock_write):
    df = pd.DataFrame({"a": [1]})
    make_loader().write_table(df, "silver", "fact_results")
    args, _ = mock_write.call_args
    assert args[0] == f"{BASE}/Tables/silver_fact_results"
