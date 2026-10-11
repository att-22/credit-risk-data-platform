-- Comportamiento de pago de cuotas agregado por cliente.
-- Granularidad: una fila por cliente (SK_ID_CURR).

{{ agregar_por_cliente(
    relacion=ref('int_installments_payments__con_comportamiento'),
    clave='SK_ID_CURR',
    prefijo='INST',
) }}
