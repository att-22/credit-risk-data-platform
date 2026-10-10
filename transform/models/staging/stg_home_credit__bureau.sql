-- Créditos previos del cliente en otras entidades (buró de crédito).
-- Una fila por crédito (SK_ID_BUREAU). No necesita limpieza.

with origen as (

    select * from {{ source('bronze', 'bureau') }}

)

select * exclude (_source_file)
from origen
