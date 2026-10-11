-- Cambios de core.solicitudes capturados por Debezium, sin duplicados.
-- Granularidad: un cambio (solicitud, LSN, operación).
--
-- El consumidor es at-least-once: tras una caída puede escribir dos veces el
-- mismo cambio. El LSN (posición en el WAL de Postgres) identifica cada cambio,
-- así que se queda la primera copia. En la lectura inicial (op = 'r') todas las
-- filas comparten LSN: por eso la clave incluye la solicitud y la operación.

with cambios as (

    select * from {{ source('cdc', 'cdc_solicitudes') }}

)

select
    sk_id_curr,
    op,
    estado,
    importe,
    canal,
    actualizado_en,
    lsn,
    tx_id,
    capturado_en,
    _ingested_at
from cambios
qualify row_number() over (
    partition by sk_id_curr, lsn, op
    order by _ingested_at
) = 1
