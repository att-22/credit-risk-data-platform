# Credit Risk Data Platform

🇬🇧 [English](README.md) · 🇪🇸 **Español**

[![CI](https://github.com/att-22/credit-risk-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/att-22/credit-risk-data-platform/actions/workflows/ci.yml)

Plataforma de datos construida a partir de mi TFM de *scoring* crediticio
(dataset [Home Credit Default Risk](https://www.kaggle.com/competitions/home-credit-default-risk/data)).

El objetivo es llevar la ETL del TFM —hecha en notebooks con pandas— a un
proyecto de ingeniería de datos profesional: código versionado, testeado,
orquestado y desplegable.

**Stack:** Python 3.13 · uv · DuckDB · Parquet · dbt (dbt-duckdb, dbt_utils,
dbt_expectations) · Apache Airflow 3 · astronomer-cosmos · FastAPI · httpx ·
Redpanda (Kafka) · confluent-kafka · PostgreSQL · Debezium · Apache Spark
(PySpark) · Delta Lake · pytest · ruff · pre-commit · GitHub Actions ·
Docker / Docker Compose

## Estado

- [x] Fase 0 — Bases: Git, empaquetado, tests, lint, CI con GitHub Actions, Docker
- [x] Fase 1 — Modelado de datos con dbt + DuckDB (arquitectura medallion)
- [x] Fase 2 — Orquestación con Airflow 3 (Cosmos, Assets, tests de DAGs)
- [x] Fase 3 — Ingesta realista: incremental, API, streaming y CDC
- [ ] Fase 4 — Spark y Delta Lake (en curso: PySpark y Delta Lake hechos)
- [ ] Fase 5 — Cloud + Terraform + despliegue continuo

## Arquitectura

Arquitectura *medallion* en un lago de datos local: cada capa tiene una
responsabilidad única y se construye a partir de la anterior.

```mermaid
flowchart LR
    csv[("CSV de Kaggle<br/>8 tablas · 2,5 GB")]
    landing[("Landing zone<br/>30 entregas diarias")]
    subgraph lake["Lago de datos (DuckDB + Parquet)"]
        bronze["<b>Bronze</b><br/>Parquet · copia fiel<br/>+ trazabilidad"]
        stg["<b>Staging</b><br/>vistas · limpieza<br/>(centinelas, XNA)"]
        int["<b>Intermediate</b><br/>tablas · agregados<br/>por cliente"]
        gold["<b>Gold</b><br/>mart_scoring__clientes<br/>1 fila por solicitud"]
    end
    ml(["Modelo de scoring"])

    csv -- "simulate-source<br/>(sistema origen simulado)" --> landing
    landing -- "ingest-bronze<br/>(incremental)" --> bronze
    bronze -- dbt --> stg --> int --> gold --> ml
```

| Capa | Herramienta | Qué hace |
|---|---|---|
| **Landing** | `simulate-source` ([simulate_source.py](src/credit_risk/simulate_source.py)) o `extract-api` ([extract_api.py](src/credit_risk/extract_api.py)) | El origen simulado reparte los CSV en 30 entregas diarias (`fecha=AAAA-MM-DD/<tabla>.csv.gz`), con cada cliente y todo su historial en el mismo día, sin perder filas. Llegan como ficheros o se extraen de su API REST. |
| **Bronze** | `ingest-bronze` ([ingest.py](src/credit_risk/ingest.py)) | Ingesta **incremental**: solo las entregas que no están en la tabla de control. Parquet particionado por `fecha_entrega`, tipos fijados por un contrato de esquema ([esquemas_bronze.json](src/credit_risk/esquemas_bronze.json)), columnas `_ingested_at` y `_source_file`. Atómica e idempotente. |
| **Staging** | dbt, vistas | Limpieza sin cambiar la granularidad: el centinela `365243` de las columnas `DAYS_*` y el código `XNA` pasan a `NULL`. |
| **Intermediate** | dbt, tablas **incrementales** | Agregación del historial a una fila por cliente (media, máximo, mínimo, suma y proporción de cada categoría). Las columnas numéricas se descubren por introspección; las categorías salen de un contrato fijo (*seed*). Cada ejecución solo recalcula los clientes con datos nuevos. |
| **Gold** | dbt, tabla **incremental** | Solicitud + ratios financieros + recorte de ingresos a un tope congelado (p99,9 de train) + los 5 historiales agregados. Train y test en la misma tabla (columna `CONJUNTO`). |

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

`dbt build` ejecuta **58 tests de datos y 1 test unitario** intercalados con los modelos:

- **Claves:** `unique` / `not_null` en cada modelo y clave compuesta
  `(SK_ID_BUREAU, MONTHS_BALANCE)` en `bureau_balance`.
- **Integridad referencial** (`relationships`): todo cliente con historial
  tiene solicitud (`error`). Los créditos huérfanos del propio dataset se
  reportan como `warn`: 43.041 en `bureau_balance` y 37.422 en `pos_cash_balance`.
- **Reglas de negocio:** rangos (`EXT_SOURCE_*` ∈ [0, 1], `DAYS_BIRTH` < 0,
  cuota ≤ crédito...), valores aceptados y nº de filas del mart por conjunto.
- **Deriva y esquema:** tasa de impago entre 7 % y 9 % (`warn`), nº de
  columnas del mart fijo, categorías del origen dentro del contrato (`warn`) y
  tope de ingresos congelado vigente: el p99,9 actual a menos de un 10 % (`warn`).
- **Test unitario** de la lógica de ratios y del recorte de ingresos con datos
  inventados (sin depender de los datos reales).
- **Frescura** de las fuentes con `dbt source freshness` sobre `_ingested_at`.

### Carga incremental

Los agregados por cliente y el mart son modelos **incrementales** (`delete+insert`
por clave): cada ejecución recalcula solo los clientes con filas ingeridas
después de la última (marca de agua sobre `_ingested_at`, la hora de ingesta, no
la fecha de los datos, para no perder entregas tardías), y siempre con todo su
historial. [`comprobar_paridad_incremental.py`](scripts/comprobar_paridad_incremental.py)
carga los datos en tandas, simula una entrega tardía y comprueba que el mart es
idéntico al de una reconstrucción completa (`--full-refresh`); se ejecuta en la CI.

## Orquestación con Airflow

El pipeline se orquesta con **Apache Airflow 3** en Docker Compose. Un DAG
simula el sistema origen; la ingesta es **diaria** (cada ejecución procesa la
entrega de su fecha lógica) y está conectada con la transformación por
**Assets**: la transformación no se programa por horario, sino cuando la
ingesta publica datos nuevos en bronze.

```mermaid
flowchart LR
    subgraph org["DAG credit_risk_origen · manual (simula al banco)"]
        v[verify_data] --> g[generar_almacen] --> x[exportar_ficheros]
    end
    almacen[("Almacén del banco")]
    api(["API REST del origen<br/>FastAPI · servicio api-origen"])
    landing[("Landing zone<br/>fecha=AAAA-MM-DD")]
    subgraph ing["DAG credit_risk_ingesta · @daily, catchup"]
        c{elegir_canal} --> e["esperar_entrega<br/>(sensor)"]
        c --> a["extraer_api<br/>(paginación, reintentos)"]
        e --> i["ingest_bronze<br/>--fecha {{ ds }}"]
        a --> i
        i --> pb[publicar_bronze]
    end
    g --> almacen --> api
    x --> landing -.-> e
    api -. HTTP .-> a
    a --> landing
    bronze[("Asset<br/>home_credit_bronze")]
    subgraph tr["DAG credit_risk_transformacion · schedule = Asset bronze"]
        r[resolver_datos] --> f[dbt_source_freshness] --> d["dbt<br/>(Cosmos: 1 tarea por modelo)"] --> pm[publicar_mart]
    end
    mart[("Asset<br/>mart_scoring__clientes")]

    pb --> bronze -- dispara --> r
    pm --> mart
```

- **Ingesta por fecha lógica:** la ejecución del día X ingiere la entrega del
  día X (`{{ ds }}`), así que relanzar una ejecución (*Clear*) o hacer un
  *backfill* de un rango reprocesa exactamente esos días. Con `catchup=True`, al
  activar el DAG se crea una ejecución por cada día del periodo simulado.
- **Dos canales de entrada** (parámetro `canal`, bifurcación con `@task.branch`):
  - `ficheros`: el sensor `esperar_entrega` comprueba cada 30 s que el origen
    haya dejado la entrega completa y, mientras espera, libera su hueco
    (`mode="reschedule"`).
  - `api`: [`extract-api`](src/credit_risk/extract_api.py) descarga la entrega
    de la [API del origen](src/credit_risk/api_origen.py) (FastAPI, clave en
    `X-API-Key`, paginación por cursor) con reintentos ante 429/5xx (*backoff*
    exponencial con *jitter*, respetando `Retry-After`) y verificando el total de
    filas. Deja la entrega en la *landing* con el mismo contrato, así que el resto
    del pipeline no cambia. La API simula un 5 % de fallos para ejercitar los reintentos.
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
- **Tests de los DAGs en la CI:** 35 tests que cargan los DAGs dentro de la
  imagen de Airflow y comprueban su estructura (Assets, pool, una tarea por
  modelo del manifest, reintentos, comandos existentes en la imagen...).

## Streaming con Redpanda (Kafka)

Además de las entregas diarias, el origen puede publicar cada solicitud como un
**evento** en el topic `solicitudes` de [Redpanda](https://redpanda.com/)
(compatible con la API de Kafka). Un consumidor que corre siempre los escribe
en bronze en micro-lotes.

```mermaid
flowchart LR
    p["produce-events<br/>(DAG credit_risk_eventos)"] -- "clave = SK_ID_CURR" --> t[["topic solicitudes<br/>3 particiones"]]
    t --> c["consume-events<br/>(servicio consumidor-stream)"]
    c -- válidos --> b[("bronze/stream_&lt;tabla&gt;/<br/>micro-lotes Parquet")]
    c -- inválidos --> dlq[["topic solicitudes.dlq"]]
```

- **Productor** ([productor.py](src/credit_risk/productor.py)): `acks=all` e
  idempotencia (los reintentos de red no duplican); la clave `SK_ID_CURR` lleva
  todos los eventos de un cliente a la misma partición, en orden.
- **Consumidor** ([consumidor.py](src/credit_risk/consumidor.py)): grupo de
  consumidores con *commit* manual **después** de escribir cada micro-lote
  (*at-least-once*: ante una caída no se pierde nada, pero puede repetirse);
  validación contra el mismo contrato de esquema que la ingesta por lotes; los
  eventos inválidos van a una **dead letter queue** con el motivo en una cabecera;
  parada ordenada ante `SIGTERM`.
- **Duplicados:** se eliminan al leer por `_evento_id`, porque también pueden
  venir de republicar un día (offsets nuevos), no solo de relecturas.
- **Tests de integración** contra un Redpanda real en la CI: DLQ, caída entre la
  escritura y el *commit* (verificada con una mutación que confirma antes de
  escribir y pierde eventos) y republicación.
- Consola web de Redpanda (topics, mensajes, grupos): http://localhost:8082.

## CDC: captura de cambios de la base de datos del banco

La aplicación del banco guarda el estado vivo de cada solicitud en Postgres
(`core.solicitudes`) y lo va cambiando (recibida → en revisión → aprobada o
rechazada; algunas se borran). **Debezium** lee el WAL de Postgres por
replicación lógica y publica cada `INSERT`, `UPDATE` y `DELETE` en Redpanda, sin
cargar la base de datos con consultas y sin perder cambios intermedios.

```mermaid
flowchart LR
    pg[("Postgres<br/>core.solicitudes")] -- "WAL (pgoutput)" --> dbz["Debezium Server"]
    dbz --> t[["topic cdc.core.solicitudes<br/>op, before, after, LSN"]]
    t --> c["consume-cdc<br/>(servicio consumidor-cdc)"]
    c --> b[("bronze/cdc_solicitudes/<br/>registro de cambios")]
    b --> dbt["dbt (tag cdc)"]
    dbt --> h["historial SCD2<br/>(versiones con validez)"]
    dbt --> a["estado actual<br/>(réplica de la tabla)"]
```

- Bronze guarda el **registro completo de cambios**, no solo el estado final; el
  **LSN** (posición en el WAL) los ordena y sirve para deduplicar (el consumo es
  *at-least-once*, con el mismo bucle que el streaming).
- dbt construye el **historial SCD tipo 2** (cada versión con su intervalo de
  validez, para consultas "¿en qué estado estaba el día D?") y la **réplica** del
  estado actual. Los modelos se activan con `--vars '{con_cdc: true}'`.
- [`comprobar_replica_cdc.py`](scripts/comprobar_replica_cdc.py) compara la
  réplica con la tabla real de Postgres y verifica que cada solicitud tenga todas
  sus versiones: perder *inserts* no cambia el estado final, pero sí el historial.
  Se ejecuta en la CI con Postgres, Debezium y Redpanda reales.

## Spark

[`spark_jobs/agregados.py`](spark_jobs/agregados.py) reimplementa en PySpark las
agregaciones de las tablas más grandes (`bureau_balance`, 27 M filas;
`installments_payments`, 13,6 M) y
[`comprobar_paridad_spark.py`](scripts/comprobar_paridad_spark.py) verifica que
el resultado es idéntico al de dbt. Spark se ejecuta en un contenedor (no hace
falta Java en Windows):

```bash
docker compose run --rm --service-ports spark spark-submit /opt/spark-jobs/agregados.py --explicar
```

Con estos datos (~530 MB) DuckDB es unas 5 veces más rápido: Spark paga un coste
fijo (JVM, planificación de tareas, *shuffle*) que solo compensa cuando los datos
no caben en una máquina y hay que repartir el trabajo entre varias.

### Delta Lake

[`spark_jobs/delta_lake.py`](spark_jobs/delta_lake.py) lleva bronze y el agregado
por crédito a **tablas Delta**: Parquet más un registro de transacciones
(`_delta_log/`) que añade garantías de base de datos:

- **ACID**: cada escritura es un *commit* atómico; nunca se lee una tabla a medias.
- **`MERGE`** (*upsert*): el agregado se actualiza de forma incremental solo con los
  créditos que tienen datos nuevos; una entrega tardía **actualiza** sus créditos
  (25.390 en la prueba con el día 15) en lugar de duplicarlos.
- ***Time travel***: cada *commit* es una versión que se puede volver a leer
  (`versionAsOf`): el "corte de lectura" de la Fase 3, de forma nativa.
- **Contrato de esquema**: escribir una columna que la tabla no tiene es un error.
- **`OPTIMIZE`**: compacta los ficheros pequeños que dejan los `MERGE`.

```bash
docker compose run --rm spark spark-submit /opt/spark-jobs/delta_lake.py cargar
docker compose run --rm spark spark-submit /opt/spark-jobs/delta_lake.py historia
uv run python scripts/comprobar_paridad_spark.py --delta   # Delta (leído con deltalake) frente a dbt
```

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
uv run simulate-source          # CSV -> lake/landing (entregas diarias)
uv run ingest-bronze            # landing -> lake/bronze (solo las entregas nuevas)
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
docker compose up -d --build    # Airflow, API del origen, Redpanda, core bancario, Debezium y consumidores
```

Abre http://localhost:8080 (sin usuario: configuración solo para uso local),
lanza `credit_risk_origen` (genera las entregas del "banco": las sirve la API en
http://localhost:8000/docs y deja una copia en la *landing zone*) y activa
`credit_risk_transformacion` y `credit_risk_ingesta`: la ingesta procesa los 30
días uno a uno y cada día dispara la transformación. Por defecto se usa la
muestra; para los datos completos, lanza el origen con `datos = completo` y un
*backfill* de la ingesta con el mismo parámetro. Con `canal = api`, la ingesta
extrae las entregas de la API en lugar de esperar los ficheros. Para pararlo:
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
├── src/credit_risk/       # paquete Python: configuración, simulador del origen, ingesta,
│                          #   API del origen y extracción, streaming y CDC
├── transform/             # proyecto dbt
│   ├── models/staging/        # limpieza (vistas)
│   ├── models/intermediate/   # agregaciones por cliente (tablas incrementales)
│   ├── models/marts/          # tabla final de scoring
│   ├── models/cdc/            # historial SCD2 y réplica del core bancario
│   ├── seeds/                 # contrato de categorías
│   ├── macros/                # limpieza, agregación, corte de lectura, marca de agua
│   └── tests/                 # tests singulares
├── orchestration/         # Airflow y servicios del "banco" simulado
│   ├── Dockerfile             # imagen de Airflow + Cosmos + el proyecto (venv aparte)
│   ├── dags/                  # DAGs de origen, ingesta, transformación, eventos y core
│   ├── tests/                 # tests de integridad de los DAGs
│   ├── core_bancario/         # esquema del Postgres del banco
│   └── debezium/              # configuración de Debezium Server
├── spark/ · spark_jobs/   # imagen de Spark + Delta y trabajos de PySpark
├── compose.yaml           # todo el entorno local con Docker Compose
├── scripts/               # muestra sintética, contrato de esquema y comprobaciones de paridad
├── tests/                 # tests de pytest (unitarios y de integración) y muestra de datos
├── .github/workflows/     # CI: 9 jobs
└── Dockerfile             # imagen multi-stage del proyecto, usuario sin root
```

## Integración continua

En cada PR, GitHub Actions ejecuta en paralelo:

- **Lint:** `ruff check` y `ruff format --check`.
- **Tests:** pytest en Ubuntu y Windows.
- **dbt build (muestra):** el pipeline completo `verify-data → simulate-source → ingest-bronze → dbt build → dbt source freshness` sobre la muestra sintética, y la paridad entre la carga incremental y la reconstrucción completa.
- **Streaming (Redpanda):** arranca un broker y ejecuta los tests de integración del productor y el consumidor.
- **Spark:** tests de los trabajos de PySpark y Delta Lake (con Java 17) y paridad con dbt sobre la muestra.
- **CDC (Postgres + Debezium):** levanta Postgres, Debezium y Redpanda, captura un día de cambios y comprueba que la réplica de dbt es idéntica a Postgres.
- **Airflow DAGs:** construye la imagen de Airflow y ejecuta dentro los tests de integridad de los DAGs.
- **Docker build:** construcción de la imagen y prueba de humo.

`main` está protegida: solo se puede cambiar mediante PR, con la CI en verde y
*squash merge*.

## Datos

Los datos originales **no** están en este repositorio (pesan 2,5 GB y tienen la
licencia de la competición de Kaggle). Se leen desde la carpeta indicada en la
variable de entorno `DATA_DIR`; el lago (`lake/`) se genera localmente.
