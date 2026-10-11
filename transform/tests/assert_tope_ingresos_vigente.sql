-- Regla: el tope de ingresos congelado (var tope_ingresos) sigue representando
-- el percentil 99,9 de los ingresos de train, con un margen del 10 %.
--
-- Si los datos nuevos lo mueven más (inflación, otra población de clientes...),
-- avisa (warn): toca revisar el tope, igual que se revisa un modelo de ML
-- cuando sus datos derivan.
{{ config(severity='warn') }}

select
    quantile_cont(AMT_INCOME_TOTAL, 0.999) as PERCENTIL_ACTUAL,
    {{ var('tope_ingresos') }} as TOPE_CONGELADO
from {{ ref('stg_home_credit__application_train') }}
having abs(quantile_cont(AMT_INCOME_TOTAL, 0.999) / {{ var('tope_ingresos') }} - 1) > 0.10
