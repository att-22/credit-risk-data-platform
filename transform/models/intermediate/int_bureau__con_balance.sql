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
    bureau.*,
    balance.BB_MESES,
    balance.BB_PROP_IMPAGO
from bureau
-- LEFT JOIN: se conservan los créditos sin historial mensual (quedan a NULL)
left join balance using (SK_ID_BUREAU)
