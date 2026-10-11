-- Estado mensual de créditos al consumo (POS) y en efectivo anteriores.
-- Una fila por crédito anterior y mes (SK_ID_PREV + MONTHS_BALANCE).

with origen as (

    select * from {{ bronze_hasta_corte('pos_cash_balance') }}

)

select
    * exclude (_source_file)
    replace ({{ nulo_si_xna('NAME_CONTRACT_STATUS') }} as NAME_CONTRACT_STATUS)
from origen
