-- Resumen del historial mensual de cada crédito del buró.
-- Granularidad: una fila por crédito (SK_ID_BUREAU).
-- Pasa de 27,3 millones de filas (crédito x mes) a ~0,8 millones.

with balance as (

    select * from {{ ref('stg_home_credit__bureau_balance') }}

)

select
    SK_ID_BUREAU,
    count(*) as BB_MESES,
    -- STATUS 1..5 = meses con días de retraso (DPD); 0 = al día; C = cerrado; X = desconocido
    avg(case when STATUS in ('1', '2', '3', '4', '5') then 1.0 else 0.0 end) as BB_PROP_IMPAGO
from balance
group by SK_ID_BUREAU
