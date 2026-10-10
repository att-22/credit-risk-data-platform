-- Solicitudes actuales sin TARGET (las que habría que puntuar), limpias.
-- Misma limpieza que application_train.

with origen as (

    select * from {{ source('bronze', 'application_test') }}

),

limpio as (

    select
        * exclude (_source_file)
        replace (
            {{ nulo_si_centinela('DAYS_EMPLOYED') }} as DAYS_EMPLOYED,
            {{ nulo_si_xna('CODE_GENDER') }} as CODE_GENDER,
            {{ nulo_si_xna('ORGANIZATION_TYPE') }} as ORGANIZATION_TYPE
        )
    from origen

)

select * from limpio
