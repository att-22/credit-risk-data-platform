{#
    Macros de limpieza reutilizables.

    Una macro es una función de Jinja que GENERA código SQL. Así la regla
    "365243 significa sin dato" se escribe una sola vez y se usa en todos los
    modelos: si algún día cambia, se corrige en un único sitio.
#}

{# Valor que Home Credit usa en las columnas DAYS_* para indicar "sin dato" (1000 años). #}
{% macro centinela_dias() %}365243{% endmacro %}


{# Convierte el centinela de días en NULL. #}
{% macro nulo_si_centinela(columna) -%}
    nullif({{ columna }}, {{ centinela_dias() }})
{%- endmacro %}


{# Convierte el código de texto 'XNA' (not available) en NULL. #}
{% macro nulo_si_xna(columna) -%}
    nullif({{ columna }}, 'XNA')
{%- endmacro %}
