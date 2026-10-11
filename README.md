# Credit Risk Data Platform

[![CI](https://github.com/att-22/credit-risk-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/att-22/credit-risk-data-platform/actions/workflows/ci.yml)

Plataforma de datos construida a partir de mi TFM de *scoring* crediticio
(dataset [Home Credit Default Risk](https://www.kaggle.com/competitions/home-credit-default-risk/data)).

El objetivo es llevar la ETL del TFM —hecha en notebooks con pandas— a un
proyecto de ingeniería de datos profesional: código versionado, testeado,
orquestado y desplegable.

**Stack:** Python 3.13 · uv · DuckDB · Parquet · dbt (dbt-duckdb, dbt_utils,
dbt_expectations) · Apache Airflow 3 · astronomer-cosmos · pytest · ruff ·
pre-commit · GitHub Actions · Docker / Docker Compose

## Estado

- [x] Fase 0 — Bases: Git, empaquetado, tests, lint, CI con GitHub Actions, Docker
- [x] Fase 1 — Modelado de datos con dbt + DuckDB (arquitectura medallion)
- [x] Fase 2 — Orquestación con Airflow 3 (Cosmos, Assets, tests de DAGs)
- [ ] Fase 3 — Ingesta incremental y streaming
- [ ] Fase 4 — Procesamiento distribuido con Spark
- [ ] Fase 5 — Cloud + Terraform + despliegue continuo

## Arquitectura

Arquitectura *medallion* en un lago de datos local: cada capa tiene una
responsabilidad única y se construye a partir de la anterior.

```mermaid
flowchart LR
    csv[("CSV de Kaggle<br/>8 tablas · 2,5 GB")]
    subgraph lake["Lago de datos (DuckDB + Parquet)"]
        bronze["<b>Bronze</b><br/>Parquet · copia fiel<br/>+ trazabilidad"]
        stg["<b>Staging</b><br/>vistas · limpieza<br/>(centinelas, XNA)"]
        int["<b>Intermediate</b><br/>tablas · agregados<br/>por cliente"]
        gold["<b>Gold</b><br/>mart_scoring__clientes<br/>1 fila por solicitud"]
    end
    ml(["Modelo de scoring"])

    csv -- "ingest-bronze<br/>(Python + DuckDB)" --> bronze
    bronze -- dbt --> stg --> int --> gold --> ml
```

| Capa | Herramienta | Qué hace |
|---|---|---|
| **Bronze** | `ingest-bronze` ([ingest.py](src/credit_risk/ingest.py)) | CSV → Parquet (zstd), con columnas `_ingested_at` y `_source_file`. Escritura atómica e idempotente. 2,5 GB → ~530 MB en ~10 s. |
| **Staging** | dbt, vistas | Limpieza sin cambiar la granularidad: el centinela `365243` de las columnas `DAYS_*` y el código `XNA` pasan a `NULL`. |
| **Intermediate** | dbt, tablas | Agregación del historial a una fila por cliente (media, máximo, mínimo, suma y proporción de cada categoría) con una macro que descubre las columnas por introspección. |
| **Gold** | dbt, tabla | Solicitud + ratios financieros + recorte de ingresos al p99,9 (calculado solo con train) + los 5 historiales agregados. Train y test en la misma tabla (columna `CONJUNTO`). |

### Linaje de los modelos dbt

```mermaid
flowchart LR
    subgraph Staging
        s_tr[stg_..._application_train]
        s_te[stg_..._application_test]
        s_b[stg_..._bureau]
        s_bb[stg_..._bureau_balance]
        s_p[stg_..._previous_application]
        s_i[stg_..._installments_payments]
        s_pos[stg_..._pos_cash_balance]
        s_cc[stg_..._credit_card_balance]
    end
    subgraph Intermediate
        i_app[int_application__con_ratios]
        i_bbc[int_bureau_balance__por_credito]
        i_bcb[int_bureau__con_balance]
        i_b[int_bureau__por_cliente]
        i_p[int_previous_application__por_cliente]
        i_ic[int_installments_payments__con_comportamiento]
        i_i[int_installments_payments__por_cliente]
        i_pos[int_pos_cash_balance__por_cliente]
        i_cc[int_credit_card_balance__por_cliente]
    end
    mart[[mart_scoring__clientes]]

    s_tr & s_te --> i_app
    s_bb --> i_bbc --> i_bcb
    s_b --> i_bcb --> i_b
    s_p --> i_p
    s_i --> i_ic --> i_i
    s_pos --> i_pos
    s_cc --> i_cc
    i_app & i_b & i_p & i_i & i_pos & i_cc --> mart
```

El grafo interactivo se genera con `dbt docs generate && dbt docs serve`.

## Calidad de datos

`dbt build` ejecuta **55 tests de datos y 1 test unitario** intercalados con los modelos:

- **Claves:** `unique` / `not_null` en cada modelo y clave compuesta
  `(SK_ID_BUREAU, MONTHS_BALANCE)` en `bureau_balance`.
- **Integridad referencial** (`relationships`): todo cliente con historial
  tiene solicitud (`error`). Los créditos huérfanos del propio dataset se
  reportan como `warn`: 43.041 en `bureau_balance` y 37.422 en `pos_cash_balance`.
- **Reglas de negocio:** rangos (`EXT_SOURCE_*` ∈ [0, 1], `DAYS_BIRTH` < 0,
  cuota ≤ crédito...), valores aceptados y nº de filas del mart por conjunto.
- **Deriva y esquema:** tasa de impago entre 7 % y 9 % (`warn`) y nº de
  columnas del mart fijo (detecta categorías nuevas o desaparecidas).
- **Test unitario** de la lógica de ratios y del recorte de ingresos con datos
  inventados (sin depender de los datos reales).
- **Frescura** de las fuentes con `dbt source freshness` sobre `_ingested_at`.

## Orquestación con Airflow

El pipeline se orquesta con **Apache Airflow 3** en Docker Compose. Son dos DAGs
conectados por **Assets**: la transformación no se programa por horario, sino
cuando la ingesta publica datos nuevos en bronze.

```mermaid
flowchart LR
    subgraph ing["DAG credit_risk_ingesta · diario 06:00 UTC"]
        v[verify_data] --> i[ingest_bronze] --> pb[publicar_bronze]
    end
    bronze[("Asset<br/>home_credit_bronze")]
    subgraph tr["DAG credit_risk_transformacion · schedule = Asset bronze"]
        r[resolver_datos] --> f[dbt_source_freshness] --> d["dbt<br/>(Cosmos: 1 tarea por modelo)"] --> pm[publicar_mart]
    end
    mart[("Asset<br/>mart_scoring__clientes")]

    pb --> bronze -- dispara --> r
    pm --> mart
```

- **Assets con metadatos:** el evento de bronze lleva qué conjunto se ingirió
  (`muestra` o `completo`); la transformación lo lee y procesa ese mismo conjunto.
  Cada conjunto tiene su propio lago, así una prueba nunca pisa los datos completos.
- **[astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos):** lee el
  `manifest.json` de dbt y crea una tarea por modelo (con sus tests) y una por
  cada test que depende de varios modelos. Cada modelo tiene sus propios logs y
  reintentos, y se puede relanzar solo una parte del grafo.
- **Pool `duckdb` de 1 hueco:** DuckDB admite un único proceso escribiendo, así
  que las tareas que lo usan se ejecutan de una en una.
- **Imagen propia:** Airflow y el proyecto (dbt, pandas...) viven en entornos
  virtuales separados dentro de la imagen para que sus dependencias no choquen.
- **Tests de los DAGs en la CI:** 13 tests que cargan los DAGs dentro de la
  imagen de Airflow y comprueban su estructura (Assets, pool, una tarea por
  modelo del manifest, reintentos...).

## Hallazgos sobre los datos

- **Centinela 365243** (= 1000 años) significa "sin dato" en 6 columnas
  `DAYS_*`. En `DAYS_EMPLOYED` son 55.374 solicitudes (jubilados y
  desempleados). Sin limpiarlo, la media pasa de −2.384 a +63.815 días.
- **Hay un cliente real con `SK_ID_CURR = 365243`.** El TFM reemplazaba el
  centinela en toda la tabla y le borraba el historial. Aquí se limpia
  columna a columna.
- **Paridad con el TFM:** 0 diferencias numéricas en las variables comunes. Las
  únicas discrepancias están explicadas: `sum()` de todo nulos (0 en pandas,
  `NULL` en SQL), el cliente 365243, y céntimos en `INST_PAGO_DIF` porque el
  TFM restaba en float32.

## Puesta en marcha

Requisitos: [uv](https://docs.astral.sh/uv/). uv instala la versión de Python adecuada.

```bash
uv sync                         # entorno virtual + dependencias (uv.lock)
cp .env.example .env            # y ajusta DATA_DIR a tu carpeta de datos
```

### Probar sin descargar los datos de Kaggle

El repositorio incluye una **muestra sintética** (314 clientes, 2,3 MB) en
[tests/fixtures/muestra](tests/fixtures/muestra), generada con
[scripts/crear_muestra.py](scripts/crear_muestra.py). Conserva las relaciones
entre tablas, los tipos y las categorías, pero los valores de cada columna
están barajados, así que ninguna fila corresponde a un cliente real. Es la que
usa la CI.

```bash
export DATA_DIR=tests/fixtures/muestra
uv run verify-data              # comprueba los CSV de entrada
uv run ingest-bronze            # CSV -> lake/bronze/*.parquet
cd transform
uv run dbt deps                 # paquetes de dbt
uv run dbt build                # modelos + tests (~2 s con la muestra)
```

### Con los datos completos

Descarga los CSV de [Kaggle](https://www.kaggle.com/competitions/home-credit-default-risk/data)
en `<DATA_DIR>/raw/`, define `DATA_DIR` en `.env` y ejecuta los mismos comandos
(sin el `export`). Con los datos completos, `dbt build` tarda unos 25 s.

### Airflow

```bash
docker compose up -d --build    # Postgres + Airflow (api-server, scheduler, dag-processor)
```

Abre http://localhost:8080 (sin usuario: configuración solo para uso local),
activa los DAGs `credit_risk_ingesta` y `credit_risk_transformacion`, y lanza la
ingesta con **Trigger** eligiendo `datos = muestra` o `completo`. La
transformación arranca sola al terminar la ingesta. Para pararlo:
`docker compose down` (los volúmenes con el lago y el historial se conservan).

Compose lee `DATA_DIR` del archivo `.env`; con `datos = muestra` no hacen falta
los datos de Kaggle (la muestra sintética va dentro de la imagen).

### Docker

```bash
docker build -t credit-risk .
docker run --rm -v "<ruta a DATA_DIR>:/data:ro" credit-risk   # verify-data
```

En Git Bash (Windows) hay que anteponer `MSYS_NO_PATHCONV=1` a `docker run`.

## Estructura

```
├── src/credit_risk/       # paquete Python: configuración, verificación e ingesta
├── transform/             # proyecto dbt
│   ├── models/staging/        # limpieza (vistas)
│   ├── models/intermediate/   # agregaciones por cliente (tablas)
│   ├── models/marts/          # tabla final de scoring
│   ├── macros/                # limpieza y agregar_por_cliente
│   └── tests/                 # tests singulares
├── orchestration/         # Airflow
│   ├── Dockerfile             # imagen de Airflow + Cosmos + el proyecto (venv aparte)
│   ├── dags/                  # DAGs de ingesta y transformación
│   └── tests/                 # tests de integridad de los DAGs
├── compose.yaml           # Airflow en local con Docker Compose
├── scripts/               # utilidades (generar la muestra sintética)
├── tests/                 # tests de pytest y muestra de datos
├── .github/workflows/     # CI: lint, tests (Linux + Windows), dbt, DAGs de Airflow, Docker
└── Dockerfile             # imagen multi-stage, usuario sin root
```

## Integración continua

En cada PR, GitHub Actions ejecuta en paralelo:

- **Lint:** `ruff check` y `ruff format --check`.
- **Tests:** pytest en Ubuntu y Windows.
- **dbt build (muestra):** el pipeline completo `verify-data → ingest-bronze → dbt build → dbt source freshness` sobre la muestra sintética.
- **Airflow DAGs:** construye la imagen de Airflow y ejecuta dentro los tests de integridad de los DAGs.
- **Docker build:** construcción de la imagen y prueba de humo.

`main` está protegida: solo se puede cambiar mediante PR, con la CI en verde y
*squash merge*.

## Datos

Los datos originales **no** están en este repositorio (pesan 2,5 GB y tienen la
licencia de la competición de Kaggle). Se leen desde la carpeta indicada en la
variable de entorno `DATA_DIR`; el lago (`lake/`) se genera localmente.
