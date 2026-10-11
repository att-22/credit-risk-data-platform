-- Historial de versiones de cada solicitud: SCD tipo 2 construido a partir del CDC.
-- Granularidad: una fila por versión (solicitud, LSN del cambio que la creó).
--
-- SCD 2 (Slowly Changing Dimension de tipo 2): en vez de sobrescribir, cada
-- cambio crea una versión nueva con su intervalo de validez. Permite responder
-- "¿en qué estado estaba la solicitud X el día D?" (consultas "as of").
--
-- Cada insert/update/lectura abre una versión, que se cierra cuando llega el
-- siguiente cambio de la misma solicitud (orden por LSN, el orden real del WAL).
-- Un delete no abre versión: solo cierra la anterior con la hora del borrado.

with cambios as (

    select * from {{ ref('stg_core__solicitudes_cambios') }}

),

con_siguiente as (

    select
        *,
        lead(op) over por_solicitud as op_siguiente,
        -- Fin de la versión: la hora del siguiente cambio (la del borrado si es un delete,
        -- que no tiene actualizado_en propio: trae el de la fila anterior)
        lead(case when op = 'd' then capturado_en else actualizado_en end)
            over por_solicitud as fin
    from cambios
    window por_solicitud as (partition by sk_id_curr order by lsn)

)

select
    sk_id_curr,
    estado,
    importe,
    canal,
    actualizado_en as valido_desde,
    fin as valido_hasta,
    fin is null as es_actual,
    coalesce(op_siguiente = 'd', false) as cerrada_por_borrado,
    lsn
from con_siguiente
where op <> 'd'
