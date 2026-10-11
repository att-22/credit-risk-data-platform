-- Pagos de cuotas de créditos anteriores en Home Credit.
-- Una fila por pago (puede haber varios pagos por cuota). No necesita limpieza.

with origen as (

    select * from {{ bronze_hasta_corte('installments_payments') }}

)

select * exclude (_source_file)
from origen
