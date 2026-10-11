{#
    Lee una tabla de bronze, opcionalmente hasta un instante de corte.

    Problema: una ejecución de dbt (y más aún con Cosmos, una tarea por modelo)
    lee bronze en momentos distintos. Si la ingesta sigue escribiendo entre
    medias, unos modelos ven más días que otros y los tests que los comparan
    fallan (el mart tiene menos filas que staging, p. ej.).

    Solución: con la variable `corte_ingesta` (un timestamp), todos los modelos
    leen solo las filas ingeridas hasta ese instante: la misma "foto" de los
    datos durante toda la ejecución. Lo que llegue después lo recoge la siguiente
    ejecución gracias a la marca de agua incremental. Sin la variable, se lee todo.

    Uso en staging:  select * from {{ bronze_hasta_corte('bureau') }}
#}

{% macro bronze_hasta_corte(tabla) -%}
    {%- set corte = var('corte_ingesta', none) -%}
    {%- if corte -%}
        (select * from {{ source('bronze', tabla) }}
         where _INGESTED_AT <= '{{ corte }}'::TIMESTAMPTZ)
    {%- else -%}
        {{ source('bronze', tabla) }}
    {%- endif -%}
{%- endmacro %}
