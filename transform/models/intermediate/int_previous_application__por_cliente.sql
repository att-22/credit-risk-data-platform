-- Solicitudes anteriores en Home Credit agregadas por cliente.
-- Granularidad: una fila por cliente (SK_ID_CURR).

{{ agregar_por_cliente(
    relacion=ref('stg_home_credit__previous_application'),
    clave='SK_ID_CURR',
    prefijo='PREV',
    categoricas=['NAME_CONTRACT_STATUS', 'NAME_CONTRACT_TYPE', 'NAME_YIELD_GROUP'],
) }}
