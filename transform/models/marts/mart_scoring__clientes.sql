-- Tabla final para el modelo de scoring (capa gold).
-- Granularidad: una fila por solicitud (SK_ID_CURR), de train y de test.
--   CONJUNTO = 'train' -> con TARGET, para entrenar
--   CONJUNTO = 'test'  -> sin TARGET, las solicitudes a puntuar
--
-- Las categóricas de application se dejan como texto: el one-hot y la
-- eliminación de columnas constantes son decisiones del modelo de ML, no de los
-- datos, y se hacen en el pipeline de entrenamiento.
--
-- Incremental: cada ejecución recalcula solo los clientes cuya solicitud o
-- cuyo historial se ha ingerido después de la última ejecución, y sustituye
-- sus filas (delete+insert por SK_ID_CURR).
{{ config(materialized='incremental', unique_key='SK_ID_CURR', incremental_strategy='delete+insert') }}

with solicitudes as (

    select * from {{ ref('int_application__con_ratios') }}

),

buro as (

    select * from {{ ref('int_bureau__por_cliente') }}

),

previas as (

    select * from {{ ref('int_previous_application__por_cliente') }}

),

cuotas as (

    select * from {{ ref('int_installments_payments__por_cliente') }}

),

pos as (

    select * from {{ ref('int_pos_cash_balance__por_cliente') }}

),

tarjetas as (

    select * from {{ ref('int_credit_card_balance__por_cliente') }}

)

{%- set partes = ['solicitudes', 'buro', 'previas', 'cuotas', 'pos', 'tarjetas'] %}

{% if is_incremental() %}
,

-- Clientes con algo nuevo en cualquiera de las partes desde la última ejecución
afectados as (
    {%- for parte in partes %}
    select SK_ID_CURR from {{ parte }}
    where _INGESTED_AT > (select {{ marca_de_agua() }})
    {%- if not loop.last %}
    union{% endif %}
    {%- endfor %}
)
{% endif %}

-- LEFT JOIN: un cliente sin historial en una tabla conserva su fila, con las
-- variables de esa tabla a NULL. Cada int_* tiene una fila por cliente (test
-- unique), así que los JOIN no pueden duplicar solicitudes.
select
    -- _INGESTED_AT del mart: la ingesta más reciente de cualquiera de sus partes
    -- (greatest ignora los NULL de los clientes sin historial en alguna tabla)
    solicitudes.* replace (
        greatest(
            {%- for parte in partes %}
            {{ parte }}._INGESTED_AT{{ "," if not loop.last }}
            {%- endfor %}
        ) as _INGESTED_AT
    ),
    {%- for parte in partes[1:] %}
    {{ parte }}.* exclude (SK_ID_CURR, _INGESTED_AT){{ "," if not loop.last }}
    {%- endfor %}
from solicitudes
{%- for parte in partes[1:] %}
left join {{ parte }} using (SK_ID_CURR)
{%- endfor %}
{%- if is_incremental() %}
where solicitudes.SK_ID_CURR in (select SK_ID_CURR from afectados)
{%- endif %}
