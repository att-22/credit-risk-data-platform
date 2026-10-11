-- Solicitudes anteriores en Home Credit, limpias.
-- Una fila por solicitud anterior (SK_ID_PREV).
-- Aquí el centinela 365243 aparece en 5 columnas de fechas y 'XNA' en 10 de
-- texto: en vez de escribir 15 líneas casi iguales, las generamos con bucles.

{% set columnas_dias = [
    'DAYS_FIRST_DRAWING',
    'DAYS_FIRST_DUE',
    'DAYS_LAST_DUE_1ST_VERSION',
    'DAYS_LAST_DUE',
    'DAYS_TERMINATION',
] %}

{# 'XAP' (not applicable) NO se toca: en CODE_REJECT_REASON significa
   "no rechazada", es una categoría con significado propio. #}
{% set columnas_xna = [
    'NAME_CONTRACT_TYPE',
    'NAME_CASH_LOAN_PURPOSE',
    'NAME_PAYMENT_TYPE',
    'CODE_REJECT_REASON',
    'NAME_CLIENT_TYPE',
    'NAME_GOODS_CATEGORY',
    'NAME_PORTFOLIO',
    'NAME_PRODUCT_TYPE',
    'NAME_SELLER_INDUSTRY',
    'NAME_YIELD_GROUP',
] %}

with origen as (

    select * from {{ bronze_hasta_corte('previous_application') }}

),

limpio as (

    select
        * exclude (_source_file)
        replace (
            {%- for columna in columnas_dias %}
            {{ nulo_si_centinela(columna) }} as {{ columna }},
            {%- endfor %}
            {%- for columna in columnas_xna %}
            {{ nulo_si_xna(columna) }} as {{ columna }}{{ "," if not loop.last }}
            {%- endfor %}
        )
    from origen

)

select * from limpio
