-- Resumen del historial mensual de cada crédito del buró.
-- Granularidad: una fila por crédito (SK_ID_BUREAU).
-- Pasa de 27,3 millones de filas (crédito x mes) a ~0,8 millones.

-- Incremental: solo se recalculan los créditos con meses ingeridos después de
-- la última ejecución (con todos sus meses) y se sustituyen sus filas.
{{ config(materialized='incremental', unique_key='SK_ID_BUREAU', incremental_strategy='delete+insert') }}

with balance as (

    select * from {{ ref('stg_home_credit__bureau_balance') }}

)

select
    SK_ID_BUREAU,
    count(*) as BB_MESES,
    -- STATUS 1..5 = meses con días de retraso (DPD); 0 = al día; C = cerrado; X = desconocido
    avg(case when STATUS in ('1', '2', '3', '4', '5') then 1.0 else 0.0 end) as BB_PROP_IMPAGO,
    max(_INGESTED_AT) as _INGESTED_AT
from balance
{% if is_incremental() %}
where SK_ID_BUREAU in (
    select SK_ID_BUREAU from balance
    where _INGESTED_AT > (select max(_INGESTED_AT) from {{ this }})
)
{% endif %}
group by SK_ID_BUREAU
