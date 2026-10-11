-- Base de datos operacional SIMULADA del banco ("core bancario").
-- Se ejecuta una vez, al crear el contenedor (docker-entrypoint-initdb.d).
--
-- Guarda el estado vivo de cada solicitud: la aplicación del banco la inserta
-- al recibirla y la actualiza según avanza (en revisión, aprobada, rechazada...).
-- Esa tabla cambia constantemente: es lo que captura el CDC (Debezium).

CREATE SCHEMA core;

CREATE TABLE core.solicitudes (
    sk_id_curr     BIGINT PRIMARY KEY,
    estado         TEXT NOT NULL CHECK (estado IN
                     ('recibida', 'en_revision', 'aprobada', 'rechazada')),
    importe        NUMERIC(14, 2) NOT NULL,
    canal          TEXT NOT NULL,
    actualizado_en TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL: en los UPDATE y DELETE, el WAL guarda la fila ANTERIOR
-- completa (no solo la clave). Así cada evento de CDC lleva el "antes" y el
-- "después" y se puede ver qué cambió.
ALTER TABLE core.solicitudes REPLICA IDENTITY FULL;

-- Usuario de solo lectura con permiso de replicación para Debezium:
-- el CDC nunca necesita escribir en la base de datos de la aplicación.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT USAGE ON SCHEMA core TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO debezium;

-- Publicación: qué tablas se envían por replicación lógica (protocolo pgoutput)
CREATE PUBLICATION cdc_core FOR TABLE core.solicitudes;
