{#
    Versión SQL de la función agregar_por_cliente() del TFM (src/features.py).

    Agrupa una tabla de historial por la clave indicada y genera:
      - media, máximo, mínimo y suma de cada columna numérica,
      - la proporción de cada categoría de las columnas categóricas indicadas
        (equivalente a get_dummies + mean de pandas),
      - el número de registros por grupo,
      - _INGESTED_AT: la ingesta más reciente de las filas del grupo (la usa la
        carga incremental como marca de agua).

    Las columnas NUMÉRICAS se descubren consultando la base de datos
    (introspección). Las CATEGORÍAS, en cambio, salen de un contrato fijo
    (seeds/categorias_agregacion.csv): en un modelo incremental el esquema no
    puede depender de los datos que hayan llegado hasta hoy, o una categoría
    nueva crearía una columna que los clientes antiguos no tendrían.

    Carga incremental: solo se recalculan los clientes con filas ingeridas
    después de la última ejecución, y con TODAS sus filas (no solo las nuevas),
    así el resultado es correcto aunque el historial de un cliente llegue
    repartido en varias entregas.

    Argumentos:
      relacion     -> modelo a agregar, p. ej. ref('stg_home_credit__bureau')
      clave        -> columna de agrupación, p. ej. 'SK_ID_CURR'
      prefijo      -> prefijo de las columnas generadas, p. ej. 'BURO'
      categoricas  -> columnas de texto a convertir en proporciones
#}

{% macro agregar_por_cliente(relacion, clave, prefijo, categoricas=[]) %}

    {#- Fuera de `if execute` para que dbt registre la dependencia con el seed -#}
    {%- set contrato = ref('categorias_agregacion') -%}

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

        {#- 2) Proporción de cada categoría del contrato -#}
        {%- for categorica in categoricas %}
            {%- set consulta -%}
                select valor from {{ contrato }}
                where prefijo = '{{ prefijo }}' and columna = '{{ categorica }}'
                order by valor
            {%- endset -%}
            {%- set valores = run_query(consulta).columns[0].values() if execute else [] -%}
            {%- for valor in valores %}
        , avg(case when {{ categorica }} = '{{ valor }}' then 1.0 else 0.0 end)
            as {{ prefijo }}_{{ categorica | upper }}_{{ nombre_columna(valor) }}_PROP
            {%- endfor %}
        {%- endfor %}

        , max(_INGESTED_AT) as _INGESTED_AT

    from {{ relacion }}
    {%- if is_incremental() %}
    -- Incremental: clientes con alguna fila ingerida después de la última ejecución
    where {{ clave }} in (
        select {{ clave }} from {{ relacion }}
        where _INGESTED_AT > (select {{ marca_de_agua() }})
    )
    {%- endif %}
    group by {{ clave }}

{% endmacro %}


{# Convierte un valor de texto en un trozo de nombre de columna válido:
   'Consumer credit' -> 'CONSUMER_CREDIT', 'Cash loan (non-earmarked)' -> 'CASH_LOAN_NON_EARMARKED' #}
{% macro nombre_columna(valor) -%}
    {{ modules.re.sub('_+', '_', modules.re.sub('[^0-9A-Za-z]', '_', valor | string)).strip('_') | upper }}
{%- endmacro %}
