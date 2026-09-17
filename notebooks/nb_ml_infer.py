"""
Fabric Notebook: XGBoost pit stop classifier - batch inference
Paste this into a Fabric Notebook. Loads the production model from
Fabric ML Experiments and writes predictions to mart_pitstop_predictions.
Triggered by dag_ml_predict after dag_transform_gold finishes.
"""

import mlflow
import mlflow.xgboost
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime

LAKEHOUSE = "f1_lakehouse"          # predictions are still written back to the Lakehouse
MODEL_NAME = "f1_pitstop_classifier"

# la mart de Gold vive en el Warehouse pero los archivos Delta estan en OneLake, asi que
# leo por path ABFSS directo (spark.read.synapsesql no anda en este runtime de fabric).
# el schema queda como dbo_gold (dbo + el +schema: gold de dbt).
WORKSPACE_ID = "758d3890-e301-4686-936f-7383b23dd657"
WAREHOUSE_ID = "5dfcb799-4ec5-4e63-bd5a-48600d6e3c89"   # f1_warehouse
GOLD_SCHEMA  = "dbo_gold"
FEATURES_PATH = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
    f"{WAREHOUSE_ID}/Tables/{GOLD_SCHEMA}/mart_pitstop_features"
)

FEATURE_COLS = [
    "compound_encoded",
    "tyre_age_at_pit",
    "lap_time_degradation_slope",
    "qualifying_position",
    "n_stops_so_far",
    "circuit_avg_pit_time_loss",
    "weather_is_dry",
    "constructor_avg_pitstop_duration",
]
LABEL_CLASSES = ["EARLY", "MID", "LATE"]

# --- cargar el modelo de produccion ---

# el plugin de MLflow de fabric no tiene ni alias (404 en esa api) ni stages (que
# ademas estan deprecados). Entonces resuelvo "produccion" como la version mas alta
# registrada: nb_ml_train solo registra una version si supera el umbral de F1, asi
# que la mas nueva siempre es la buena.
client = mlflow.MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
if not versions:
    raise ValueError(f"No registered versions of '{MODEL_NAME}'. Run nb_ml_train first.")
model_version = max(int(v.version) for v in versions)
model_uri = f"models:/{MODEL_NAME}/{model_version}"
model = mlflow.xgboost.load_model(model_uri)
print(f"Loaded model version: {model_version} (highest registered)")

# --- cargar la tabla de features (Warehouse, via OneLake) ---

features_spark = spark.read.format("delta").load(FEATURES_PATH)

# infiero solo sobre el año mas reciente disponible
max_year = features_spark.agg(F.max("year")).collect()[0][0]
features_spark = features_spark.filter(F.col("year") == max_year)

df = features_spark.toPandas()
print(f"Running inference on {len(df)} rows (year={max_year})")

X = df[FEATURE_COLS].fillna(0).values

# --- predecir ---

probs = model.predict_proba(X)
predicted_idx = np.argmax(probs, axis=1)
predicted_labels = [LABEL_CLASSES[i] for i in predicted_idx]

df["predicted_window"]  = predicted_labels
df["prob_early"]        = probs[:, 0]
df["prob_mid"]          = probs[:, 1]
df["prob_late"]         = probs[:, 2]
df["model_version"]     = str(model_version)
df["inference_ts"]      = datetime.utcnow()

# --- escribir las predicciones de vuelta en Gold ---

output_cols = [
    "year", "round", "race_name", "country", "driver_id", "constructor_id",
    "predicted_window", "prob_early", "prob_mid", "prob_late",
    "pit_window_class",  # el label real, lo dejo para poder comparar accuracy en Power BI
    "model_version", "inference_ts",
]

predictions_df = df[output_cols]
predictions_spark = spark.createDataFrame(predictions_df)

# f1_lakehouse tiene schemas habilitados, con nombre de tabla simple cae en el schema
# default (Tables/dbo/), igual que en el notebook de Silver. Si pongo
# "f1_lakehouse.<tabla>" resuelve mal en un lakehouse con schemas.
predictions_spark.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("gold_mart_pitstop_predictions")
print(f"Predictions written: {len(predictions_df)} rows to gold_mart_pitstop_predictions")
