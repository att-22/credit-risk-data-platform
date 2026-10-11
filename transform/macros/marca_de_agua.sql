{#
    Marca de agua de un modelo incremental: la ingesta más reciente que ya
    contiene ({{ this }}). Si la tabla está vacía (p. ej. la primera ejecución no
    encontró datos), max() es NULL y "_INGESTED_AT > NULL" nunca se cumple: el
    modelo se quedaría vacío para siempre. Con -infinity se procesa todo.
#}
{% macro marca_de_agua() -%}
    coalesce((select max(_INGESTED_AT) from {{ this }}), '-infinity'::TIMESTAMPTZ)
{%- endmacro %}
