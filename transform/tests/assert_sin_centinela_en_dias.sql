-- Test "singular": una consulta que devuelve las filas QUE INCUMPLEN la regla.
-- Si devuelve 0 filas, el test pasa; si devuelve alguna, falla.
--
-- Regla: tras la limpieza de staging, ninguna columna de días puede contener
-- el centinela 365243.

select 'application_train' as tabla, SK_ID_CURR as id
from {{ ref('stg_home_credit__application_train') }}
where DAYS_EMPLOYED = {{ centinela_dias() }}

union all

select 'application_test', SK_ID_CURR
from {{ ref('stg_home_credit__application_test') }}
where DAYS_EMPLOYED = {{ centinela_dias() }}

union all

select 'previous_application', SK_ID_PREV
from {{ ref('stg_home_credit__previous_application') }}
where {{ centinela_dias() }} in (
    DAYS_FIRST_DRAWING,
    DAYS_FIRST_DUE,
    DAYS_LAST_DUE_1ST_VERSION,
    DAYS_LAST_DUE,
    DAYS_TERMINATION
)
