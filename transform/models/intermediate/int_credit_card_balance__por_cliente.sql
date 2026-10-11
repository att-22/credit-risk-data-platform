-- Saldos mensuales de tarjetas de crédito agregados por cliente.
-- Granularidad: una fila por cliente (SK_ID_CURR).

{{ agregar_por_cliente(
    relacion=ref('stg_home_credit__credit_card_balance'),
    clave='SK_ID_CURR',
    prefijo='CC',
    categoricas=['NAME_CONTRACT_STATUS'],
) }}
