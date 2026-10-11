{#
    Versión SQL de la función agregar_por_cliente() del TFM (src/features.py).

    Agrupa una tabla de historial por la clave indicada y genera:
      - media, máximo, mínimo y suma de cada columna numérica,
      - la proporción de cada categoría de las columnas categóricas indicadas
        (equivalente a get_dummies + mean de pandas),
      - el número de registros por grupo.

    Las columnas se descubren CONSULTANDO la base de datos (introspección), así
    que si la tabla de origen gana una columna numérica, se agrega sola.

    Argumentos:
      relacion     -> modelo a agregar, p. ej. ref('stg_home_credit__bureau')
      clave        -> columna de agrupación, p. ej. 'SK_ID_CURR'
      prefijo      -> prefijo de las columnas generadas, p. ej. 'BURO'
      categoricas  -> lista de columnas de texto a convertir en proporciones
#}

{% macro agregar_por_cliente(relacion, clave, prefijo, categoricas=[]) %}

    {#- Identificadores y metadatos: no tiene sentido sumarlos ni promediarlos -#}
    {%- set excluidas = ['SK_ID_CURR', 'SK_ID_BUREAU', 'SK_ID_PREV', '_INGESTED_AT'] -%}
    {%- set funciones = {'MEAN': 'avg', 'MAX': 'max', 'MIN': 'min', 'SUM': 'sum'} -%}

    {#- Columnas numéricas de la relación (solo disponibles en la fase de ejecución) -#}
    {%- set numericas = [] -%}
    {%- if execute -%}
        {%- for columna in adapter.get_columns_in_relation(relacion) -%}
            {%- if columna.is_number() and columna.name | upper not in excluidas -%}
                {%- do numericas.append(columna.name) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    select
        {{ clave }},
        count(*) as {{ prefijo }}_NUM_REGISTROS

        {#- 1) Estadísticos de cada columna numérica -#}
        {%- for columna in numericas %}
            {%- for sufijo, funcion in funciones.items() %}
        , {{ funcion }}({{ columna }}) as {{ prefijo }}_{{ columna | upper }}_{{ sufijo }}
            {%- endfor %}
        {%- endfor %}

        {#- 2) Proporción de cada categoría: se consultan sus valores distintos -#}
        {%- for categorica in categoricas %}
            {%- set consulta -%}
                select distinct {{ categorica }} from {{ relacion }}
                where {{ categorica }} is not null order by 1
            {%- endset -%}
            {%- set valores = run_query(consulta).columns[0].values() if execute else [] -%}
            {%- for valor in valores %}
        , avg(case when {{ categorica }} = '{{ valor }}' then 1.0 else 0.0 end)
            as {{ prefijo }}_{{ categorica | upper }}_{{ nombre_columna(valor) }}_PROP
            {%- endfor %}
        {%- endfor %}

    from {{ relacion }}
    group by {{ clave }}

{% endmacro %}


{# Convierte un valor de texto en un trozo de nombre de columna válido:
   'Consumer credit' -> 'CONSUMER_CREDIT', 'Cash loan (non-earmarked)' -> 'CASH_LOAN_NON_EARMARKED' #}
{% macro nombre_columna(valor) -%}
    {{ modules.re.sub('_+', '_', modules.re.sub('[^0-9A-Za-z]', '_', valor | string)).strip('_') | upper }}
{%- endmacro %}
