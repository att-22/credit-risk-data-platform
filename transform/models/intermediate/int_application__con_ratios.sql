-- Solicitudes actuales (train + test) con el recorte de ingresos y los ratios
-- financieros del TFM (limpiar_application + anadir_ratios en src/features.py).
-- Granularidad: una fila por solicitud (SK_ID_CURR).

-- Solo se usa para construir el mart: no merece guardarse como tabla.
{{ config(materialized='view') }}

with train as (

    select * from {{ ref('stg_home_credit__application_train') }}

),

test as (

    select * from {{ ref('stg_home_credit__application_test') }}

),

-- Tope de ingresos: percentil 99,9 de AMT_INCOME_TOTAL en train, CONGELADO como
-- parámetro (var tope_ingresos). Si se recalculara con los datos de cada día,
-- cambiaría con cada entrega y, en un mart incremental, las filas antiguas
-- quedarían con un tope distinto de las nuevas. Es lo que se hace en producción
-- con todo lo que se "ajusta" sobre datos de entrenamiento: se fija hasta el
-- próximo reentrenamiento. El test assert_tope_ingresos_vigente avisa si los
-- datos se alejan de él.
tope as (

    select {{ var('tope_ingresos') }}::DOUBLE as TOPE_INGRESOS

),

-- Train y test en una sola relación. "union all by name" (DuckDB) empareja las
-- columnas por nombre: test no tiene TARGET y queda a NULL.
solicitudes as (

    select 'train' as CONJUNTO, * from train
    union all by name
    select 'test' as CONJUNTO, * from test

),

recortado as (

    select
        solicitudes.* replace (
            case
                when AMT_INCOME_TOTAL > tope.TOPE_INGRESOS then tope.TOPE_INGRESOS
                else AMT_INCOME_TOTAL
            end as AMT_INCOME_TOTAL
        )
    from solicitudes
    cross join tope

)

select
    *,
    -- nullif(x, 0): en DuckDB dividir entre 0 da infinito; así queda NULL,
    -- igual que el replace([inf, -inf], nan) del TFM.
    AMT_CREDIT / nullif(AMT_INCOME_TOTAL, 0) as RATIO_CREDITO_INGRESOS,
    AMT_ANNUITY / nullif(AMT_INCOME_TOTAL, 0) as RATIO_CUOTA_INGRESOS,
    AMT_ANNUITY / nullif(AMT_CREDIT, 0) as RATIO_CUOTA_CREDITO,
    AMT_INCOME_TOTAL / nullif(CNT_FAM_MEMBERS, 0) as INGRESO_POR_PERSONA,
    DAYS_EMPLOYED / nullif(DAYS_BIRTH, 0) as PORC_VIDA_EMPLEADO,

    -- Resúmenes por fila de las tres puntuaciones externas. Las funciones list_*
    -- ignoran los NULL, como pandas con skipna (std con ddof=1 = stddev_samp).
    list_avg([EXT_SOURCE_1, EXT_SOURCE_2, EXT_SOURCE_3]) as EXT_SOURCE_MEDIA,
    list_min([EXT_SOURCE_1, EXT_SOURCE_2, EXT_SOURCE_3]) as EXT_SOURCE_MIN,
    list_max([EXT_SOURCE_1, EXT_SOURCE_2, EXT_SOURCE_3]) as EXT_SOURCE_MAX,
    list_stddev_samp([EXT_SOURCE_1, EXT_SOURCE_2, EXT_SOURCE_3]) as EXT_SOURCE_DESV
from recortado
