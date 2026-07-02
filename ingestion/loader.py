import pandas as pd
from deltalake import write_deltalake
from deltalake.writer import WriterProperties


class OneLakeLoader:
    """Writes pandas DataFrames as Delta tables to Microsoft Fabric OneLake via ABFS."""

    def __init__(self, storage_options: dict, lakehouse_abfss: str):
        # storage_options: azure credentials for ABFS access
        # lakehouse_abfss: base path like abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>
        self.storage_options = storage_options
        self.base = lakehouse_abfss.rstrip("/")

    def _write(self, df: pd.DataFrame, path: str, mode: str = "overwrite"):
        if df is None or df.empty or len(df.columns) == 0:
            return
        write_deltalake(
            path,
            df,
            mode=mode,
            storage_options=self.storage_options,
            writer_properties=WriterProperties(compression="snappy"),
        )

    def write_bronze(self, df: pd.DataFrame, table: str, year: int, round_number: int):
        path = f"{self.base}/Tables/bronze_{table}/year={year}/round={round_number}"
        self._write(df, path, mode="overwrite")

    def write_bronze_dim(self, df: pd.DataFrame, table: str, year: int):
        path = f"{self.base}/Tables/bronze_{table}/year={year}"
        self._write(df, path, mode="overwrite")

    def write_table(self, df: pd.DataFrame, layer: str, table: str, mode: str = "overwrite"):
        path = f"{self.base}/Tables/{layer}_{table}"
        self._write(df, path, mode=mode)
