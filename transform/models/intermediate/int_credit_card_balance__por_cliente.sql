-- Saldos mensuales de tarjetas de crédito agregados por cliente.
-- Granularidad: una fila por cliente (SK_ID_CURR).

-- Incremental: cada ejecución recalcula solo los clientes con datos nuevos
-- (ver la macro) y sustituye sus filas (delete+insert por SK_ID_CURR).
{{ config(materialized='incremental', unique_key='SK_ID_CURR', incremental_strategy='delete+insert') }}

{{ agregar_por_cliente(
    relacion=ref('stg_home_credit__credit_card_balance'),
    clave='SK_ID_CURR',
    prefijo='CC',
    categoricas=['NAME_CONTRACT_STATUS'],
) }}
