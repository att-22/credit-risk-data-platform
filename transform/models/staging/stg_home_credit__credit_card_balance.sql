-- Estado mensual de tarjetas de crédito anteriores.
-- Una fila por tarjeta y mes (SK_ID_PREV + MONTHS_BALANCE). No necesita limpieza.

with origen as (

    select * from {{ bronze_hasta_corte('credit_card_balance') }}

)

select * exclude (_source_file)
from origen
