-- Regla: toda categoría que llega del origen está en el contrato de categorías
-- (seeds/categorias_agregacion.csv). Si no, sus filas no cuentan en ninguna
-- columna *_PROP y las proporciones del cliente dejan de sumar 1.
--
-- Solo avisa (warn): el pipeline sigue funcionando, pero alguien tiene que
-- decidir si la categoría nueva se añade al contrato.
{{ config(severity='warn') }}

with observadas as (

    select distinct 'BURO' as prefijo, 'CREDIT_ACTIVE' as columna, CREDIT_ACTIVE as valor
    from {{ ref('stg_home_credit__bureau') }}
    union
    select distinct 'BURO', 'CREDIT_TYPE', CREDIT_TYPE
    from {{ ref('stg_home_credit__bureau') }}
    union
    select distinct 'PREV', 'NAME_CONTRACT_STATUS', NAME_CONTRACT_STATUS
    from {{ ref('stg_home_credit__previous_application') }}
    union
    select distinct 'PREV', 'NAME_CONTRACT_TYPE', NAME_CONTRACT_TYPE
    from {{ ref('stg_home_credit__previous_application') }}
    union
    select distinct 'PREV', 'NAME_YIELD_GROUP', NAME_YIELD_GROUP
    from {{ ref('stg_home_credit__previous_application') }}
    union
    select distinct 'POS', 'NAME_CONTRACT_STATUS', NAME_CONTRACT_STATUS
    from {{ ref('stg_home_credit__pos_cash_balance') }}
    union
    select distinct 'CC', 'NAME_CONTRACT_STATUS', NAME_CONTRACT_STATUS
    from {{ ref('stg_home_credit__credit_card_balance') }}

)

select observadas.*
from observadas
left join {{ ref('categorias_agregacion') }} as contrato
    using (prefijo, columna, valor)
where observadas.valor is not null
  and contrato.valor is null
