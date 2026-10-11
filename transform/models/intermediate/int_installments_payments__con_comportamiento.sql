-- Pagos de cuotas con variables de comportamiento de pago.
-- Granularidad: una fila por pago, igual que installments_payments.

-- Paso intermedio que solo se usa para agregar: no merece guardarse como tabla.
{{ config(materialized='view') }}

with pagos as (

    select * from {{ ref('stg_home_credit__installments_payments') }}

)

select
    *,
    AMT_INSTALMENT - AMT_PAYMENT                    as PAGO_DIF,
    -- nullif(x, 0): si la cuota es 0, el ratio queda NULL en vez de dividir por cero
    AMT_PAYMENT / nullif(AMT_INSTALMENT, 0)         as PAGO_RATIO,
    -- Días de retraso / adelanto respecto al vencimiento (0 si no aplica, NULL si no hay pago)
    case
        when DAYS_ENTRY_PAYMENT is null then null
        else greatest(DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT, 0)
    end                                             as DIAS_RETRASO,
    case
        when DAYS_ENTRY_PAYMENT is null then null
        else greatest(DAYS_INSTALMENT - DAYS_ENTRY_PAYMENT, 0)
    end                                             as DIAS_ADELANTO
from pagos
