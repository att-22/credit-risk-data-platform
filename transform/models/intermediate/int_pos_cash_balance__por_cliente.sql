-- Saldos mensuales de créditos al consumo agregados por cliente.
-- Granularidad: una fila por cliente (SK_ID_CURR).

{{ agregar_por_cliente(
    relacion=ref('stg_home_credit__pos_cash_balance'),
    clave='SK_ID_CURR',
    prefijo='POS',
    categoricas=['NAME_CONTRACT_STATUS'],
) }}
