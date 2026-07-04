"""
Fabric Notebook: XGBoost pit stop classifier — training
Paste this into a Fabric Notebook. Uses Fabric ML Experiments (MLflow) for tracking.
Run manually or via dag_train_ml after a new season of Gold data is available.
"""

import mlflow
import mlflow.xgboost
import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

# Gold marts live in the Fabric Warehouse (dbt writes them there). Warehouse tables are
# stored as Delta in OneLake, so Spark reads them by ABFSS path with the delta format.
# (spark.read.synapsesql is NOT available in this Fabric Spark runtime.)
# dbt writes the marts under schema `dbo` + `+schema: gold` = `dbo_gold`.
WORKSPACE_ID = "8bdbcee8-5387-4ad5-a7db-e92c73250b76"
WAREHOUSE_ID = "c603802a-0c47-446d-b328-a4acaabed970"   # f1_warehouse
GOLD_SCHEMA  = "dbo_gold"
FEATURES_PATH = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
    f"{WAREHOUSE_ID}/Tables/{GOLD_SCHEMA}/mart_pitstop_features"
)
EXPERIMENT_NAME = "f1_pitstop_classifier"
F1_THRESHOLD = 0.65   # minimum F1-score to register the model as production

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
LABEL_COL = "pit_window_class"

# ── Load feature table from Gold (Fabric Warehouse, via OneLake Delta path) ──

df_spark = spark.read.format("delta").load(FEATURES_PATH)
df = df_spark.toPandas()

print(f"Total samples: {len(df)}")
print(f"Class distribution:\n{df[LABEL_COL].value_counts()}")

# ── Temporal split (MUST be by year to avoid leakage) ────────────────────────

train_df = df[df["year"].isin([2021, 2022])].copy()
val_df   = df[df["year"] == 2023].copy()
test_df  = df[df["year"] == 2024].copy()

print(f"Train: {len(train_df)} | Validation: {len(val_df)} | Test: {len(test_df)}")

# ── Encode label ─────────────────────────────────────────────────────────────

le = LabelEncoder()
le.fit(["EARLY", "MID", "LATE"])

y_train = le.transform(train_df[LABEL_COL])
y_val   = le.transform(val_df[LABEL_COL])
y_test  = le.transform(test_df[LABEL_COL])

X_train = train_df[FEATURE_COLS].fillna(0).values
X_val   = val_df[FEATURE_COLS].fillna(0).values
X_test  = test_df[FEATURE_COLS].fillna(0).values

# class weights to handle imbalance
class_counts = np.bincount(y_train)
sample_weights = np.array([1 / class_counts[y] for y in y_train])

# ── Hyperparameter search (grid search on validation set) ────────────────────

param_grid = {
    "n_estimators":  [100, 200, 500],
    "max_depth":     [3, 5, 7],
    "learning_rate": [0.01, 0.1],
    "subsample":     [0.8, 1.0],
}

base_model = XGBClassifier(
    objective="multi:softprob",
    num_class=3,
    eval_metric="mlogloss",
    use_label_encoder=False,
    random_state=42,
    n_jobs=-1,
)

gs = GridSearchCV(
    base_model,
    param_grid,
    cv=5,
    scoring="f1_macro",
    verbose=1,
    n_jobs=-1,
)

gs.fit(X_train, y_train, sample_weight=sample_weights)

best_params = gs.best_params_
print(f"Best params: {best_params}")

# ── Train final model with best params ───────────────────────────────────────

model = XGBClassifier(
    objective="multi:softprob",
    num_class=3,
    eval_metric="mlogloss",
    use_label_encoder=False,
    random_state=42,
    n_jobs=-1,
    **best_params,
)
model.fit(X_train, y_train, sample_weight=sample_weights)

# ── Evaluate ─────────────────────────────────────────────────────────────────

def evaluate(X, y_true, split_name):
    y_pred = model.predict(X)
    y_prob = model.predict_proba(X)
    f1     = f1_score(y_true, y_pred, average="macro")
    prec   = precision_score(y_true, y_pred, average="macro", zero_division=0)
    rec    = recall_score(y_true, y_pred, average="macro", zero_division=0)
    auc    = roc_auc_score(y_true, y_prob, multi_class="ovr", average="macro")
    print(f"{split_name} — F1: {f1:.3f} | Precision: {prec:.3f} | Recall: {rec:.3f} | AUC: {auc:.3f}")
    return {"f1": f1, "precision": prec, "recall": rec, "auc": auc}, y_pred

train_metrics, _ = evaluate(X_train, y_train, "Train")
val_metrics,   _ = evaluate(X_val,   y_val,   "Validation")
test_metrics, y_test_pred = evaluate(X_test, y_test, "Test")

# ── MLflow logging ───────────────────────────────────────────────────────────

mlflow.set_experiment(EXPERIMENT_NAME)

with mlflow.start_run(run_name=f"xgboost_{best_params['n_estimators']}est"):

    mlflow.log_params(best_params)
    mlflow.log_param("label_classes", list(le.classes_))
    mlflow.log_param("features", FEATURE_COLS)
    mlflow.log_param("train_years", "2021-2022")
    mlflow.log_param("test_year", "2024")

    for split, metrics in [("train", train_metrics), ("val", val_metrics), ("test", test_metrics)]:
        for metric_name, value in metrics.items():
            mlflow.log_metric(f"{split}_{metric_name}", value)

    # confusion matrix plot
    cm = confusion_matrix(y_test, y_test_pred)
    fig, ax = plt.subplots(figsize=(6, 5))
    sns.heatmap(cm, annot=True, fmt="d", xticklabels=le.classes_, yticklabels=le.classes_, ax=ax)
    ax.set_xlabel("Predicted")
    ax.set_ylabel("Actual")
    ax.set_title("Confusion Matrix — Test Set (2024)")
    mlflow.log_figure(fig, "confusion_matrix.png")
    plt.close()

    # feature importance plot
    fig2, ax2 = plt.subplots(figsize=(8, 5))
    importances = pd.Series(model.feature_importances_, index=FEATURE_COLS).sort_values(ascending=True)
    importances.plot(kind="barh", ax=ax2, color="steelblue")
    ax2.set_title("XGBoost Feature Importance (Gain)")
    ax2.set_xlabel("Importance")
    mlflow.log_figure(fig2, "feature_importance.png")
    plt.close()

    # log model artifact
    mlflow.xgboost.log_model(model, artifact_path="model", registered_model_name=None)

    run_id = mlflow.active_run().info.run_id

    # register as production only if test F1 meets the threshold
    if test_metrics["f1"] >= F1_THRESHOLD:
        mlflow.register_model(
            model_uri=f"runs:/{run_id}/model",
            name="f1_pitstop_classifier",
            tags={"stage": "production"},
        )
        print(f"Model registered as production — F1={test_metrics['f1']:.3f} >= {F1_THRESHOLD}")
    else:
        print(f"Model NOT registered — F1={test_metrics['f1']:.3f} < {F1_THRESHOLD} threshold")
        raise ValueError(f"Model quality below threshold. Check experiment '{EXPERIMENT_NAME}' in Fabric ML Experiments.")
