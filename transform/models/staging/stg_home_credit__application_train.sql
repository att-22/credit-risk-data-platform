-- Solicitudes actuales (con TARGET), limpias.
-- Una fila por solicitud (SK_ID_CURR). Sin agregaciones ni variables nuevas:
-- staging solo limpia y normaliza.

with origen as (

    select * from {{ bronze_hasta_corte('application_train') }}

),

limpio as (

    select
        * exclude (_source_file)
        replace (
            -- 365243 = "sin empleo" (jubilados y desempleados)
            {{ nulo_si_centinela('DAYS_EMPLOYED') }} as DAYS_EMPLOYED,
            -- 'XNA' = "no disponible"
            {{ nulo_si_xna('CODE_GENDER') }} as CODE_GENDER,
            {{ nulo_si_xna('ORGANIZATION_TYPE') }} as ORGANIZATION_TYPE
        )
    from origen

)

select * from limpio
