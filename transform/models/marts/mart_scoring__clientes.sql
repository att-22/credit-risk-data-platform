-- Tabla final para el modelo de scoring (capa gold).
-- Granularidad: una fila por solicitud (SK_ID_CURR), de train y de test.
--   CONJUNTO = 'train' -> con TARGET, para entrenar
--   CONJUNTO = 'test'  -> sin TARGET, las solicitudes a puntuar
--
-- Las categóricas de application se dejan como texto: el one-hot y la
-- eliminación de columnas constantes son decisiones del modelo de ML, no de los
-- datos, y se hacen en el pipeline de entrenamiento.

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

-- LEFT JOIN: un cliente sin historial en una tabla conserva su fila, con las
-- variables de esa tabla a NULL. Cada int_* tiene una fila por cliente (test
-- unique), así que los JOIN no pueden duplicar solicitudes.
select *
from solicitudes
left join buro using (SK_ID_CURR)
left join previas using (SK_ID_CURR)
left join cuotas using (SK_ID_CURR)
left join pos using (SK_ID_CURR)
left join tarjetas using (SK_ID_CURR)
