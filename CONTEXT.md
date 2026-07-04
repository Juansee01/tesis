# TFM F1 Analytics — Contexto de sesión 2026-07-03

## Proyecto
TFM Data Engineering Master UCM. Pipeline completo de F1:
- Fuentes: fastf1 (telemetría) + Jolpica-F1/Ergast API (resultados)
- Arquitectura: Bronze -> Silver -> Gold (medallion) en Microsoft Fabric OneLake
- ML: XGBoost predictor de ventanas de pit stop (EARLY/MID/LATE)
- Orquestación: Apache Airflow 2.9.3 en Docker local
- Repo: https://github.com/Juansee01/tesis

## Fabric (UCM license, sin Service Principal)
- Workspace ID: `8bdbcee8-5387-4ad5-a7db-e92c73250b76`
- Lakehouse ID: `62643acc-3951-41db-a299-4505be982467`
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
7. `dag_train_ml` — entrena XGBoost (necesita paso 5).
8. `dag_ml_predict` — inferencia batch, escribe `mart_pitstop_predictions`.
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
