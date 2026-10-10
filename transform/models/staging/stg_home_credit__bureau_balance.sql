-- Estado mensual de cada crédito del buró.
-- Una fila por crédito y mes (SK_ID_BUREAU + MONTHS_BALANCE).

with origen as (

    select * from {{ source('bronze', 'bureau_balance') }}

)

select * exclude (_source_file)
from origen
