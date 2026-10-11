-- Regla: el mart tiene exactamente las mismas solicitudes que staging en cada
-- conjunto, y TARGET está informado en train y vacío en test.
-- Detecta JOIN que duplican o pierden filas y mezclas entre train y test.

with esperado as (

    select 'train' as CONJUNTO, count(*) as FILAS
    from {{ ref('stg_home_credit__application_train') }}
    union all
    select 'test', count(*)
    from {{ ref('stg_home_credit__application_test') }}

),

obtenido as (

    select
        CONJUNTO,
        count(*) as FILAS,
        count(*) filter (where (CONJUNTO = 'train') = (TARGET is null)) as TARGET_INCORRECTO
    from {{ ref('mart_scoring__clientes') }}
    group by CONJUNTO

)

select
    esperado.CONJUNTO,
    esperado.FILAS as FILAS_ESPERADAS,
    obtenido.FILAS as FILAS_OBTENIDAS,
    obtenido.TARGET_INCORRECTO
from esperado
full join obtenido using (CONJUNTO)
where esperado.FILAS is distinct from obtenido.FILAS
   or obtenido.TARGET_INCORRECTO > 0
