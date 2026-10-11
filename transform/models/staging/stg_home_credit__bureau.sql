-- Créditos previos del cliente en otras entidades (buró de crédito).
-- Una fila por crédito (SK_ID_BUREAU). No necesita limpieza.

with origen as (

    select * from {{ bronze_hasta_corte('bureau') }}

)

select * exclude (_source_file)
from origen
