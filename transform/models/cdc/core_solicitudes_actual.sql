-- Estado actual de cada solicitud que sigue existiendo en el core bancario.
-- Granularidad: una fila por solicitud (SK_ID_CURR).
--
-- Es una réplica de core.solicitudes reconstruida solo con el CDC: las
-- versiones vigentes del historial (las borradas ya están cerradas). La prueba
-- de integración la compara con la tabla real de Postgres.

select
    sk_id_curr,
    estado,
    importe,
    canal,
    valido_desde as actualizado_en
from {{ ref('int_core__solicitudes_historial') }}
where es_actual
