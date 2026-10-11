"""Contrato de los eventos de streaming y utilidades comunes de Kafka.

Un evento es una solicitud de crédito nueva que el sistema origen publica en el
topic `solicitudes` en cuanto se produce (en lugar de esperar a la entrega del
día). Lleva la fila con el texto original y unos metadatos:

    {
      "evento_id": "2026-09-07/application_train/100002",
      "tabla": "application_train",        # application_train o application_test
      "conjunto": "muestra",
      "fecha_entrega": "2026-09-07",
      "publicado_en": "2026-10-11T18:00:00+00:00",
      "datos": {"SK_ID_CURR": "100002", "TARGET": "1", ...}
    }

La validación usa el MISMO contrato de esquema que la ingesta por lotes
(esquemas_bronze.json): un evento cuyas columnas no coincidan, o cuyo
SK_ID_CURR no sea un entero, es inválido y va a la cola de errores (DLQ).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import BaseModel, ValidationError, field_validator, model_validator

from credit_risk.config import KAFKA_BOOTSTRAP, TOPIC_DLQ, TOPIC_SOLICITUDES
from credit_risk.ingest import ESQUEMAS

TABLAS_EVENTO = ("application_train", "application_test")
PARTICIONES = 3  # el topic se reparte en 3 particiones; la clave (SK_ID_CURR) decide cuál


class Evento(BaseModel):
    evento_id: str
    tabla: Literal["application_train", "application_test"]
    conjunto: str
    fecha_entrega: date
    publicado_en: datetime
    datos: dict[str, str | None]

    @field_validator("datos")
    @classmethod
    def identificador_entero(cls, datos: dict) -> dict:
        valor = datos.get("SK_ID_CURR")
        if valor is None or not valor.isdigit():
            raise ValueError(f"SK_ID_CURR no es un entero: {valor!r}")
        return datos

    @model_validator(mode="after")
    def columnas_del_contrato(self) -> Evento:
        esperadas = list(ESQUEMAS[self.tabla])
        if list(self.datos) != esperadas:
            nuevas = sorted(set(self.datos) - set(esperadas))
            faltan = sorted(set(esperadas) - set(self.datos))
            raise ValueError(f"columnas distintas del contrato: nuevas {nuevas}, faltan {faltan}")
        return self


def validar(valor: bytes) -> tuple[Evento | None, str | None]:
    """Devuelve (evento, None) si es válido o (None, motivo) si no."""
    try:
        return Evento.model_validate_json(valor), None
    except ValidationError as error:
        return None, error.errors()[0]["msg"][:300]


def asegurar_topics(bootstrap: str = KAFKA_BOOTSTRAP) -> None:
    """Crea los topics si no existen (crear uno que ya existe no es un error)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existentes = admin.list_topics(timeout=10).topics
    nuevos = [
        NewTopic(nombre, num_partitions=particiones, replication_factor=1)
        for nombre, particiones in ((TOPIC_SOLICITUDES, PARTICIONES), (TOPIC_DLQ, 1))
        if nombre not in existentes
    ]
    if nuevos:
        for futuro in admin.create_topics(nuevos).values():
            futuro.result()
