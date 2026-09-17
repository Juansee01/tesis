# F1 Analytics Platform

Trabajo Fin de Máster (Máster en Big Data & Data Engineering, UCM). Plataforma de
análisis y predicción de datos de Fórmula 1 sobre arquitectura lakehouse en
Microsoft Fabric, con un pipeline ELT end-to-end (2019-2024, 128 Grandes Premios)
y un clasificador XGBoost para la ventana de pit stop.

La memoria y el video del TFM se entregan por separado (fuera de este repositorio),
según el formato de entrega del máster.

## Arquitectura

```
fastf1 / Jolpica-F1 (Ergast)
        |
        v
librería de ingesta (ingestion/)  --API REST Fabric-->  Apache Airflow (Docker local)
        |
        v
Fabric OneLake (Delta Lake): Bronze -> Silver -> Gold
        |                         |
   PySpark Notebooks           dbt (Silver -> Gold)
        |
        v
Gold: mart_pitstop_features -> XGBoost (Fabric Notebooks + Fabric ML Experiments)
        |
        v
Fabric SQL Endpoint -> Power BI
```

- **Ingesta**: librería Python propia (`ingestion/`) sobre fastf1 y la API
  Jolpica-F1 (sucesora de Ergast).
- **Orquestación**: Apache Airflow 2.9.3, desplegado localmente con
  `docker compose` (`airflow/`).
- **Transformación**: Fabric Notebooks PySpark para Bronze -> Silver
  (`notebooks/nb_bronze_to_silver.py`); dbt para Silver -> Gold (`dbt/`).
- **ML**: clasificador XGBoost multiclase para la ventana de pit stop
  (EARLY/MID/LATE), entrenado y servido desde Fabric Notebooks
  (`notebooks/nb_ml_train.py`, `notebooks/nb_ml_infer.py`) con tracking en
  Fabric ML Experiments (MLflow).
- **Reporting**: Power BI sobre el Fabric SQL Endpoint.

## Estructura del repositorio

```
ingestion/    librería Python de ingesta (fastf1 + Jolpica-F1 -> OneLake Bronze)
airflow/      DAGs y docker-compose para orquestar el pipeline
dbt/          modelos y tests dbt (Silver -> Gold)
notebooks/    código PySpark/ML para pegar en Fabric Notebooks
tests/        tests unitarios pytest (ingestion/)
```

## Desarrollo

```bash
pip install -e ".[dev]"

# lint + formato
flake8 ingestion/ tests/ --max-line-length=100 --ignore=E501,W503
black --check ingestion/ tests/ --line-length=100

# tests + cobertura
pytest --cov=ingestion --cov-report=term-missing --cov-fail-under=80
```

CI (GitHub Actions, `.github/workflows/ci.yml`): lint + tests con cobertura
mínima del 80% sobre `ingestion/`, build del paquete en cada merge a `main`, y
validación de sintaxis de los modelos dbt.
