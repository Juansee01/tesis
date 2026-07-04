"""
Fabric Notebook: XGBoost pit stop classifier — batch inference
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
WAREHOUSE = "f1_warehouse"          # Gold feature marts live in the Warehouse
MODEL_NAME = "f1_pitstop_classifier"

FEATURE_COLS = [
    "compound_encoded",
    "tyre_age_at_pit",
    "lap_time_degradation_slope",
    "race_progress_at_pit",
    "qualifying_position",
    "n_stops_so_far",
    "circuit_avg_pit_time_loss",
    "weather_is_dry",
    "constructor_avg_pitstop_duration",
]
LABEL_CLASSES = ["EARLY", "MID", "LATE"]

# ── Load production model ─────────────────────────────────────────────────────

model_uri = f"models:/{MODEL_NAME}/production"
model = mlflow.xgboost.load_model(model_uri)
model_version = mlflow.MlflowClient().get_latest_versions(MODEL_NAME, stages=["production"])[0].version
print(f"Loaded model version: {model_version}")

# ── Load feature table (Fabric Warehouse) ─────────────────────────────────────

features_spark = spark.read.synapsesql(f"{WAREHOUSE}.dbo_gold.mart_pitstop_features")

# infer on the most recent year available
max_year = features_spark.agg(F.max("year")).collect()[0][0]
features_spark = features_spark.filter(F.col("year") == max_year)

df = features_spark.toPandas()
print(f"Running inference on {len(df)} rows (year={max_year})")

X = df[FEATURE_COLS].fillna(0).values

# ── Predict ───────────────────────────────────────────────────────────────────

probs = model.predict_proba(X)
predicted_idx = np.argmax(probs, axis=1)
predicted_labels = [LABEL_CLASSES[i] for i in predicted_idx]

df["predicted_window"]  = predicted_labels
df["prob_early"]        = probs[:, 0]
df["prob_mid"]          = probs[:, 1]
df["prob_late"]         = probs[:, 2]
df["model_version"]     = str(model_version)
df["inference_ts"]      = datetime.utcnow()

# ── Write predictions back to Gold ───────────────────────────────────────────

output_cols = [
    "year", "round", "driver_id", "constructor_id",
    "predicted_window", "prob_early", "prob_mid", "prob_late",
    "pit_window_class",  # actual label — useful for post-hoc accuracy analysis in Power BI
    "model_version", "inference_ts",
]

predictions_df = df[output_cols]
predictions_spark = spark.createDataFrame(predictions_df)

predictions_spark.write.format("delta").mode("overwrite").saveAsTable(
    f"{LAKEHOUSE}.gold_mart_pitstop_predictions"
)
print(f"Predictions written: {len(predictions_df)} rows to gold_mart_pitstop_predictions")
