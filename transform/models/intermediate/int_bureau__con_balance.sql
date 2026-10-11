-- Créditos del buró enriquecidos con el resumen de su historial mensual.
-- Granularidad: una fila por crédito (SK_ID_BUREAU), igual que bureau.

-- Paso intermedio que solo se usa para agregar: no merece guardarse como tabla.
{{ config(materialized='view') }}

with bureau as (

    select * from {{ ref('stg_home_credit__bureau') }}

),

balance as (

    select * from {{ ref('int_bureau_balance__por_credito') }}

)

select
    -- _INGESTED_AT: la ingesta más reciente del crédito o de su historial mensual,
    -- para que la carga incremental detecte cambios en cualquiera de los dos
    bureau.* replace (
        greatest(bureau._INGESTED_AT, balance._INGESTED_AT) as _INGESTED_AT
    ),
    balance.BB_MESES,
    balance.BB_PROP_IMPAGO
from bureau
-- LEFT JOIN: se conservan los créditos sin historial mensual (quedan a NULL)
left join balance using (SK_ID_BUREAU)
