# TFM F1 Analytics — Contexto de sesión 2026-07-06

## Proyecto
TFM Data Engineering Master UCM. Pipeline completo de F1:
- Fuentes: fastf1 (telemetría) + Jolpica-F1/Ergast API (resultados)
- Arquitectura: Bronze -> Silver -> Gold (medallion) en Microsoft Fabric OneLake
- ML: XGBoost predictor de ventanas de pit stop (EARLY/MID/LATE)
- Orquestación: Apache Airflow 2.9.3 en Docker local
- Repo: https://github.com/Juansee01/tesis

## >>> RETOMAR AQUÍ (2026-07-06) <<<
Pipeline end-to-end VERDE. Estado exacto para continuar:
- **ML train + infer funcionan en Fabric.** Modelo `f1_pitstop_classifier` registrado
  hasta version 3+ (cada run sube versión). Infer escribió 608 predicciones (2024).
- **DAGs `dag_train_ml` y `dag_ml_predict` en SUCCESS** (runs 2026-07-06 16:02 y 16:06).
- **DECISIÓN PENDIENTE #1 — LEAKAGE del label (bloqueante para la memoria).**
  `race_progress_at_pit = pit_lap/total_laps` (feature) y el label `pit_window_class`
  (bucket de `pit_lap`: <=20 EARLY, <=40 MID, else LATE) salen de la MISMA variable
  `pit_lap` -> leakage trivial -> por eso AUC 0.988. Fix propuesto (NO aplicado aún):
  sacar `race_progress_at_pit` de `FEATURE_COLS` en `nb_ml_train.py` + `nb_ml_infer.py`
  (8 features, no 9); NO tocar el mart (el label se sigue calculando en dbt). Luego
  re-pegar ambos notebooks en Fabric, re-correr, reportar AUC honesto (bajará). Alternativa
  débil: justificar el leakage en la redacción sin re-entrenar. El resto de features
  (tyre_age_at_pit, compound, grid, n_stops_so_far, deg slope) son estado legítimo pre-pit.
- **PENDIENTE #2 — Power BI:** 4 dashboards (lo hace el usuario en Power BI Desktop).
  Marts Gold en el SQL endpoint del WAREHOUSE `f1_warehouse` (schema `dbo_gold`);
  predicciones en el SQL endpoint del LAKEHOUSE (`gold_mart_pitstop_predictions`, Tables/dbo).
- Entorno: Airflow arriba en localhost:8080 (admin/admin); `az login` OK juanliza@ucm.es
  (token ~1h). El usuario corre los DAGs desde la UI; el asistente NO dispara dag runs.

## Fabric (UCM license, sin Service Principal)
- Workspace ID: `8bdbcee8-5387-4ad5-a7db-e92c73250b76`
- Lakehouse ID: `62643acc-3951-41db-a299-4505be982467`
- Warehouse ID: `c603802a-0c47-446d-b328-a4acaabed970` (f1_warehouse)
- Workspace name en Spark: `f1_analytics`
- Lakehouse name: `f1_lakehouse`
- Warehouse name: `f1_warehouse`  (NUEVO — destino de escritura de Gold, ver decisión abajo)
- Auth: `az login` con cuenta UCM (juanliza@ucm.es) + `AzureCliCredential` en Docker
- `.azure` montado en container sin `:ro` (necesita escribir logs de comandos)
- SQL host (compartido por Lakehouse y Warehouse del workspace):
  `y6oqok7k4k6elemcb7pbjnkjwe-5dhnxc4hkpkuvj635ewhgjiloy.datawarehouse.fabric.microsoft.com`

## Notebooks en Fabric
- `nb_bronze_to_silver`: `333459e4-effd-43e4-a3b8-2ab8c60443e9`
- `nb_ml_train`: `e2495f84-246a-42e4-9404-d934fc9ab073`
- `nb_ml_infer`: `c44b1651-37c7-4ca8-b3e2-dc7333d0d067`

## Decisión clave Gold: dbt escribe en un Fabric WAREHOUSE, no en el Lakehouse (2026-07-03)
El SQL Analytics Endpoint de un Lakehouse es READ-ONLY: `dbt run` daba
`external policy action '...Tables/Create' was denied (368)`. En un Lakehouse las
escrituras van por Spark; el T-SQL DDL (lo que hace dbt) requiere un Warehouse.

Solución (se mantiene dbt, no se toca la narrativa dbt de la memoria):
- Se creó el Warehouse `f1_warehouse` en el workspace.
- dbt ESCRIBE los marts Gold en `f1_warehouse` (`database: f1_warehouse` en profiles).
- dbt LEE Silver del Lakehouse cross-database via three-part name
  `f1_lakehouse.dbo.silver_*` (`database: f1_lakehouse` en el source de sources.yml).
- Mismo SQL host para ambos (workspace-shared); solo cambia `database`.

## Estado actual

### COMPLETADO
1. **Bronze ingestion** — `dag_ingest_f1` verde (2021-2024, 91 GPs)
   - Datos en OneLake: `Tables/bronze_*/year=YYYY/round=RR` (Delta separados por año/round)
2. **Silver transformation** — `dag_transform_silver` OK
   - `nb_bronze_to_silver` corrió OK en Fabric (status `Completed`)
   - Tablas Silver (en Lakehouse, prefijo `silver_`): `silver_dim_drivers`,
     `silver_dim_constructors`, `silver_dim_circuits`, `silver_fact_laps`,
     `silver_fact_pitstops`, `silver_fact_results`, `silver_fact_qualifying`
   - Conteos verificados: laps 98357, pitstops 2462, results 1799
3. **Gold (dbt -> Warehouse)** — `dbt run --select gold` PASS=4 desde host
   - 4 marts creados en `f1_warehouse.dbo`: `mart_lap_performance`,
     `mart_constructor_standings`, `mart_pitstop_features`, `mart_pitstop_strategy`
   - `mart_pitstop_features`: 2264 filas, 1 por pit stop, sin fan-out,
     3 clases presentes (EARLY/MID/LATE). Alimenta `nb_ml_train`.

### EN PROGRESO / PENDIENTE
4. **~~dbt en el container de Airflow~~ HECHO (2026-07-04)** — `dbt debug` PASS desde el
   container (`All checks passed!`, conecta a `f1_warehouse`). Cómo quedó:
   - `airflow/Dockerfile`: `msodbcsql18` + `unixodbc-dev` + `azure-cli` desde repos MS
     (azure-cli desde `repos/azure-cli/`, ODBC desde `debian/12/prod`; MISMO keyring
     `microsoft-prod.gpg`, ambas sources con `signed-by` explícito, se borran listas MS
     previas para evitar `Conflicting values Signed-By`).
   - dbt en VENV AISLADO (`/opt/dbt-venv`, `dbt-fabric==1.10.0` + `networkx>=3,<4`),
     symlink `/usr/local/bin/dbt`. NO instalar dbt en el env de airflow: sus deps chocan
     con los pins de airflow 2.9.3 -> pip `ResolutionTooDeep`. El DAG llama `dbt` por
     subprocess, solo necesita el binario en PATH.
   - `docker-compose.yml`: mounts `../dbt:/opt/dbt` y `~/.dbt:/home/airflow/.dbt`
     (profiles auth CLI). OJO: el DAG calcula `DBT_PROJECT_DIR = Path(__file__).parents[2]/"dbt"`;
     en el container `__file__=/opt/airflow/dags/...` -> `parents[2]=/opt` -> `/opt/dbt`
     (NO `/opt/airflow/dbt`). Por eso el mount va a `/opt/dbt`.
   - REQUISITO runtime: `az login` fresco en el host (token ~1h); `.azure` montado comparte
     la sesión con el container. Verificado: `az account show` -> juanliza@ucm.es.
   - VERIFICADO end-to-end (2026-07-04): DAG corrió verde (dbt_run + dbt_test success).
     El DAG queda PAUSADO; se dispara manualmente desde la UI (decisión del usuario).
5. **~~Ajustar `nb_ml_train`~~ HECHO (2026-07-04)** — leía Gold del LAKEHOUSE
   (`spark.read.format("delta").load("Tables/gold_mart_pitstop_features")`); ahora lee
   del WAREHOUSE via el conector Spark de Fabric:
   `spark.read.synapsesql("f1_warehouse.dbo.mart_pitstop_features")` (tabla SIN prefijo `gold_`).
   Mismo fix aplicado a `nb_ml_infer` (lectura de features). NOTA: `nb_ml_infer` aún
   ESCRIBE las predicciones al Lakehouse (`gold_mart_pitstop_predictions`) — el destino de
   escritura sigue pendiente (ver item 8).
6. **~~dag_validate~~ HECHO (2026-07-04)** — verificado (funciones probadas en python, sin
   ejecutar el DAG; el usuario corre los DAGs desde la UI). Dos fixes por la realidad de Fabric:
   - GOLD ya no está en el Lakehouse: se valida por SQL contra el Warehouse. Nuevo helper
     `fabric_utils.get_warehouse_connection()` (pyodbc + ODBC Driver 18 + token AAD scope
     `database.windows.net`). `validate_gold_tables` cuenta filas de los 4 marts en el
     schema `dbo_gold` (dbt: `dbo` + `+schema: gold`) y chequea integridad del feature store
     (sin null en driver_id/pit_window_class, 3 clases presentes). Requiere `pyodbc` (agregado
     a requirements) + envs `FABRIC_SQL_HOST` y `FABRIC_WAREHOUSE` (en `.env`, NO en git;
     tambien `FABRIC_GOLD_SCHEMA` opcional, default `dbo_gold`).
   - SILVER: el Lakehouse `f1_lakehouse` es SCHEMA-ENABLED -> las tablas manejadas viven en
     `Tables/dbo/silver_*`, no `Tables/silver_*`. Corregido el path ABFSS en `validate_silver_tables`.
   - Resultado: SILVER todas las cols 0% null y sin duplicados; GOLD marts con filas
     (lap_performance 1718, pitstop_strategy 2462, constructor_standings 900, pitstop_features 2264).
7. **nb_ml_train VERIFICADO en Fabric (2026-07-04)** — corrió y REGISTRÓ el modelo en producción:
   Train F1 0.994 | Val 0.904 | Test 0.891 | AUC 0.988 (>= 0.65). PENDIENTE: correr
   `dag_train_ml` desde la UI (dispara el mismo notebook por API) para las capturas.
   OJO memoria: AUC 0.988 es muy alto -> posible que `pit_window_class` esté derivada de
   `race_progress_at_pit` (feature) -> justificar definición del label / descartar leakage trivial.
8. **~~carga del modelo en Fabric~~ RESUELTO (2026-07-06)** — historia: primero se intentó por
   STAGE (`models:/.../production`) -> deprecado, no había versión en ese stage. Luego por
   ALIAS (`set_registered_model_alias` / `@production`) -> **Fabric NO implementa el alias API**:
   `set_registered_model_alias exception ... /api/2.0/mlflow/registered-models/alias 404`.
   FIX DEFINITIVO (commit `9174334`, VERIFICADO en Fabric): ni stage ni alias.
   - nb_ml_train: solo `mlflow.register_model(...)` -> sube el número de versión cuando F1>=0.65.
   - nb_ml_infer: carga la VERSIÓN MÁS ALTA:
     `client.search_model_versions("name='f1_pitstop_classifier'")` -> `max(int(v.version))` ->
     `models:/f1_pitstop_classifier/<version>`. Verificado: "Loaded model version: 3", 608 filas.
   Write predicciones: bare `saveAsTable("gold_mart_pitstop_predictions")` -> Tables/dbo/ del
   Lakehouse (2-part `f1_lakehouse.<tabla>` misresuelve en schema-enabled). Cols del mart para
   output_cols verificadas (year, round, driver_id, constructor_id, pit_window_class).
9. Power BI (4 dashboards) — apuntar al SQL endpoint del Warehouse para Gold.
10. `dbt test --select gold` — PASS=26 WARN=1 ERROR=0 (verde). El único no-PASS es
    `not_null` sobre `silver_fact_laps.lap_time`: 1640 nulls (=1.67% de ~98k laps),
    ESPERADO en F1 (in/out-laps, safety car, vueltas incompletas). Se ajustó el test en
    `sources.yml` a `warn_if:">0"` / `error_if:">1968"` (~2% SLA de nulos de la memoria):
    warnea pero no rompe mientras esté bajo el SLA. Queda pendiente (cosmético) la
    deprecation `MissingArgumentsPropertyInGenericTestDeprecation`.
11. Push final a GitHub.

### Nota para la memoria (Plantilla Memoria TFM)
La memoria dice que el Fabric SQL Endpoint del Lakehouse expone Gold. Con esta decisión,
Gold vive en el Warehouse `f1_warehouse` y se expone desde ahí. Es un ajuste chico de
redacción (Gold en Warehouse, no en el SQL endpoint del Lakehouse); dbt se mantiene igual.

## Config dbt (fuera del repo)
`~/.dbt/profiles.yml` (NO en git):
```yaml
f1_analytics:
  target: dev
  outputs:
    dev:
      type: fabric
      driver: "ODBC Driver 18 for SQL Server"
      server: "y6oqok7k4k6elemcb7pbjnkjwe-5dhnxc4hkpkuvj635ewhgjiloy.datawarehouse.fabric.microsoft.com"
      port: 1433
      database: "f1_warehouse"
      schema: dbo
      authentication: CLI          # usa token de `az`, sin browser
      encrypt: true
      trust_cert: false
      threads: 4
```

## Transformaciones T-SQL aplicadas a los marts (dialecto Spark -> T-SQL Fabric)
- `||` concat -> `concat()`
- `stddev` -> `stdev`
- `join ... using (cols)` -> `join ... on a.col = b.col ...` (T-SQL no tiene USING)
- `= true` -> `= 1` (bit)
- `mart_constructor_standings`: reestructurado con CTE `cumulative` para evitar window
  anidada dentro del ORDER BY de otra (T-SQL no lo permite)
- `mart_pitstop_strategy`: `/ r.total_laps` -> `/ nullif(r.total_laps, 0)` (divide-by-zero);
  removidos CTEs sin uso (`laps`, `circuits`, `schedule`) que referencian columnas inexistentes
- `mart_pitstop_features`:
  - CTE `laps` SIN filtro `is_valid_lap` (el lap del pit está marcado inválido en Silver;
    filtrarlo dejaba 0 features)
  - removido el re-join a `pitstops` en el SELECT final (le faltaba `driver_id` -> fan-out 3x);
    ahora usa `lap.driver_id` de `laps_at_pit`
- `sources.yml`: agregado `identifier: silver_*` (las tablas Silver tienen prefijo)
  y `database: f1_lakehouse` (lectura cross-database)

## Bugs corregidos en sesiones previas
- `AzureCliCredential`: faltaba `az` CLI en Dockerfile -> instalado
- `.azure:ro` impedía logs de `az` -> rw
- `DefaultAzureCredential` env vacías -> `ChainedTokenCredential(AzureCliCredential(), ManagedIdentityCredential())`
- DataFrame vacío Belgian GP 2021 (R12) -> guard en `loader._write()`
- `SEASON_ROUNDS[2023]=23` -> 22
- Fabric API requiere `{"executionData":{}}` -> corregido en `fabric_utils.run_notebook()`
- Bronze Delta separados por año/round -> notebook con `_load_dim()`/`_load_fact()` que unen
- `saveAsTable("f1_lakehouse.silver_*")` schema duplicado -> `saveAsTable("silver_*")` sin prefijo
- Join `on="driver_abbreviation"` -> `.alias("driver_abbreviation")`
- Fabric devuelve `"Completed"` no `"Succeeded"` -> `wait_for_notebook()` acepta ambos
- DAG silver pasaba `execution_date` -> `NotebookBadWebRequest` -> eliminado

## PRÓXIMAS TAREAS (retomar aquí, orden sugerido)
Todo lo de ML+DAGs ya está VERDE (ver "RETOMAR AQUÍ" arriba). Quedan 2 decisiones/tareas:

1. **DECISIÓN LEAKAGE (bloqueante memoria) — ver "RETOMAR AQUÍ #1".** Si se decide corregir:
   - Editar `FEATURE_COLS` en `notebooks/nb_ml_train.py` y `notebooks/nb_ml_infer.py`:
     quitar `"race_progress_at_pit"` (quedan 8 features). NO tocar el mart dbt.
   - Re-pegar nb_ml_train en Fabric, correr -> nueva versión, AUC honesto (bajará). Re-pegar
     nb_ml_infer, correr. Re-correr `dag_train_ml` y `dag_ml_predict` desde la UI. Push.
   - Actualizar la memoria: "se detectó leakage (feature = label reescalado), se removió, métricas reales".
2. **Power BI (item 9):** 4 dashboards (Power BI Desktop, lo hace el usuario). Marts Gold en
   SQL endpoint del WAREHOUSE (schema `dbo_gold`); predicciones en SQL endpoint del LAKEHOUSE
   (`gold_mart_pitstop_predictions`, Tables/dbo).
3. **Memoria (redacción):** ajustar Gold-en-Warehouse (ya notado arriba); limpiar deprecation
   `MissingArgumentsPropertyInGenericTestDeprecation` en schema.yml (item 10, cosmético).
4. **Recordatorio operativo:** `az login` fresco en el host antes de correr DAGs (token ~1h).
   El usuario corre TODOS los DAGs desde la UI; el asistente NO dispara dag runs. Levantar entorno:
   Docker Desktop + `docker compose -f airflow/docker-compose.yml up -d`.

## Sesión 2026-07-06 (resumen)
- **ML end-to-end VERDE en Fabric.** nb_ml_train corrió (Test F1 0.891 | AUC 0.988), nb_ml_infer
  cargó version 3 y escribió 608 predicciones (2024) a `gold_mart_pitstop_predictions`.
- **Fabric MLflow NO soporta el registry alias API** (404 en `/api/2.0/mlflow/registered-models/alias`)
  ni stages (deprecados). Se reemplazó alias -> resolver por número de versión más alto.
  Commit `9174334` (`fix(ml): resolve production model by highest version...`). Ver item 8.
- **Bug DAG: Fabric reporta éxito como `Completed`, no `Succeeded`.** `dag_train_ml` y
  `dag_ml_predict` chequeaban solo `!= "Succeeded"` -> fallaban un run que en realidad terminó OK
  (dag_transform_silver ya aceptaba ambos). Fix: aceptar `("Succeeded","Completed")`.
  Commit `7ab44fe` (`fix(airflow): accept Fabric 'Completed' status...`).
- Tras el fix, ambos DAGs re-corridos desde la UI -> **SUCCESS** (`dag_train_ml` 16:02,
  `dag_ml_predict` 16:06). Nota: quedó un run viejo `scheduled__2026-06-29` de predict en `failed`
  (pre-fix, schedule viejo); borrar de la UI si se quiere lista limpia para captura.
- **LEAKAGE detectado en el label (pendiente de decisión, ver RETOMAR AQUÍ arriba).**
  `race_progress_at_pit` y `pit_window_class` derivan ambos de `pit_lap` -> AUC 0.988 inflado.

## Sesión 2026-07-04 (resumen)
- Fix lectura Gold desde Warehouse en notebooks ML:
  - `nb_ml_train`: `Tables/gold_mart_pitstop_features` (Delta Lakehouse) ->
    `spark.read.synapsesql("f1_warehouse.dbo.mart_pitstop_features")`. Desbloquea item 7 (train).
  - `nb_ml_infer`: misma lectura de features desde Warehouse; escritura de predicciones
    sigue en Lakehouse (pendiente item 8).
- Airflow levantado en host: http://localhost:8080 (admin/admin).
- dbt corriendo DENTRO del container de Airflow (item 4 HECHO): Dockerfile con ODBC Driver 18
  + azure-cli + venv dbt aislado; compose monta `../dbt` y `~/.dbt`. `dbt debug` PASS.
- `dag_transform_gold` corrido verde desde la UI por el usuario (dbt_run + dbt_test success).
- `dag_validate` (item 6 HECHO): GOLD por SQL al Warehouse (pyodbc, `get_warehouse_connection`),
  SILVER por ABFSS con path `Tables/dbo/` (Lakehouse schema-enabled). Ambas funciones verdes.
- ENVS nuevos en airflow/.env (no git): `FABRIC_SQL_HOST`, `FABRIC_WAREHOUSE`.
- REGLA: el usuario corre los DAGs desde la UI de Airflow; yo NO disparo dag runs.
- Notebooks ML leen el mart del Warehouse: `spark.read.synapsesql(...)` NO existe en el runtime
  de Fabric (`AttributeError`). Cambiado a `spark.read.format("delta").load(<abfss>)`. Las tablas
  del Warehouse son Delta en OneLake:
  `abfss://<ws>@onelake.dfs.fabric.microsoft.com/<warehouse_id>/Tables/dbo_gold/mart_pitstop_features`.
  (deltalake-rs da `DeltaProtocolError` en ese path -> protocolo nuevo; Spark sí lo lee.)
- PENDIENTE ML: pegar nb_ml_train/nb_ml_infer actualizados en Fabric y correr; luego dag_train_ml.
  nb_ml_infer todavía ESCRIBE predicciones al Lakehouse (`saveAsTable`) -> revisar path
  schema-enabled (item 8).
- BUG entrenamiento (arreglado 2026-07-04): primer run dio AUC=0.500 / Recall=0.333 (modelo
  constante, F1=0.046, no registró). Causa: `sample_weights = 1/count` daba pesos ~0.001-0.005;
  con `min_child_weight=1` (default XGBoost) los hessianos por hoja nunca llegan a 1 -> 0 splits
  -> predicción constante. Fix: `compute_sample_weight(class_weight="balanced", y=y_train)`
  (pesos promedian 1). Las features del mart SÍ tienen señal (verificado por SQL: rangos y
  distintos valores OK; solo `weather_is_dry` es constante=1, inútil pero inofensiva).

## Sesión 2026-07-03 (resumen)
- Reinstalado ODBC Driver 18 (`brew reinstall msodbcsql18`) -> OK v18.6.2.1
- Creado `~/.dbt/profiles.yml`, auth `CLI` (token `az`, sin browser); `dbt debug` OK
- Fix networkx 2.3 -> 3.6.1 (dbt corre en Python 3.14.3 via pyenv; `fractions.gcd` removido)
- Descubierto: Lakehouse SQL endpoint es read-only -> creado Warehouse `f1_warehouse`
- Reescritos los 4 marts a T-SQL; `sources.yml` cross-db; `dbt run --select gold` PASS=4
- Airflow local levantado: http://localhost:8080 (admin/admin)

## Cómo levantar el entorno
```bash
# 1. Docker Desktop corriendo (open -a Docker)
# 2. Login Azure (token expira ~1h)
az login
# 3. Airflow
cd /Users/juanse/TFM
docker compose -f airflow/docker-compose.yml up -d
# UI: localhost:8080 (admin/admin)

# dbt Gold desde host (mientras no esté en el container):
cd /Users/juanse/TFM/dbt
dbt run --select gold
```

## Estructura del proyecto
```
/Users/juanse/TFM/
├── ingestion/          # librería Python: FastF1Client, ErgastClient, OneLakeLoader
├── airflow/
│   ├── dags/           # DAGs + fabric_utils.py
│   ├── docker-compose.yml
│   ├── Dockerfile      # FALTA: dbt-fabric + ODBC Driver 18 para correr Gold en container
│   └── .env            # NO en git — IDs, FERNET_KEY
├── dbt/
│   ├── models/gold/    # 4 marts .sql (T-SQL) + sources.yml + schema.yml
│   └── profiles.yml.example
├── notebooks/          # código para pegar en Fabric
│   ├── nb_bronze_to_silver.py
│   ├── nb_ml_train.py  # FALTA: leer Gold del Warehouse, no del Lakehouse
│   └── nb_ml_infer.py
└── .github/workflows/ci.yml
```
