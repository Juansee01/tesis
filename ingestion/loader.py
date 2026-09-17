import pandas as pd
from deltalake import write_deltalake
from deltalake.writer import WriterProperties


class OneLakeLoader:
    """Writes pandas DataFrames as Delta tables to Microsoft Fabric OneLake via ABFS."""

    def __init__(self, storage_options: dict, lakehouse_abfss: str):
        # storage_options son las credenciales de azure para ABFS
        # lakehouse_abfss es algo tipo abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>
        self.storage_options = storage_options
        self.base = lakehouse_abfss.rstrip("/")

    def _write(self, df: pd.DataFrame, path: str, mode: str = "overwrite"):
        if df is None or df.empty or len(df.columns) == 0:
            return
        write_deltalake(
            path,
            df,
            mode=mode,
            # las tablas bronze/silver siempre se reemplazan enteras, nunca se les hace
            # append con un shape distinto. Por eso fuerzo el schema del dataframe actual
            # en vez de dejar el que quedo fijado en el primer CREATE TABLE. Me paso que
            # un año tenia una columna de indice de pandas de mas y se quedaba pegada para
            # siempre porque Delta no evoluciona el schema solo en un overwrite.
            schema_mode="overwrite" if mode == "overwrite" else None,
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
