-- Historial del buró de crédito agregado por cliente.
-- Granularidad: una fila por cliente (SK_ID_CURR).

{{ agregar_por_cliente(
    relacion=ref('int_bureau__con_balance'),
    clave='SK_ID_CURR',
    prefijo='BURO',
    categoricas=['CREDIT_ACTIVE', 'CREDIT_TYPE'],
) }}
