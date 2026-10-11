"""API REST del sistema origen SIMULADO (el "banco"), hecha con FastAPI.

Sirve las entregas diarias que el banco guarda en ORIGEN_RAIZ/<conjunto>/ (las
genera `simulate-source --destino ...`). Imita lo que suele encontrarse un Data
Engineer al extraer datos de una API real:

- Autenticación con una clave en la cabecera `X-API-Key` (401 si falta o no vale).
- Paginación por cursor: cada respuesta trae como mucho `limite` filas y un
  `siguiente` opaco para pedir la página siguiente (null en la última).
- Fallos transitorios simulados: con probabilidad ORIGEN_TASA_FALLOS responde
  429 (demasiadas peticiones, con cabecera Retry-After) o 503 (no disponible).
  El cliente debe reintentar; ver extract_api.py.

Endpoints:
    GET /salud
    GET /v1/{conjunto}/entregas                    -> fechas disponibles
    GET /v1/{conjunto}/entregas/{fecha}/{tabla}    -> una página de filas

La documentación interactiva (Swagger) queda en /docs.

Uso:
    uv run uvicorn credit_risk.api_origen:app --port 8000
"""

from __future__ import annotations

import base64
import os
import random
import threading
from collections import OrderedDict
from datetime import date
from pathlib import Path
from typing import Annotated

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from credit_risk.config import ORIGEN_API_KEY, ORIGEN_RAIZ, TABLES

LIMITE_MAXIMO = 10_000


class Pagina(BaseModel):
    """Una página de filas de una tabla de una entrega."""

    conjunto: str
    fecha: date
    tabla: str
    total: int  # filas totales de la tabla ese día (el cliente lo usa para verificar)
    columnas: list[str]
    filas: list[list[str | None]]  # texto original del origen; None = vacío
    siguiente: str | None  # cursor de la página siguiente; None en la última


class FalloSimulado(Exception):
    """Error transitorio provocado a propósito para ejercitar los reintentos."""

    def __init__(self, codigo: int):
        self.codigo = codigo


class Almacen:
    """Lee las entregas del banco y guarda en memoria las últimas tablas usadas.

    Cada fichero se carga una vez en DuckDB (en memoria) y se pagina con
    LIMIT/OFFSET. La clave de la caché incluye la fecha de modificación del
    fichero: si el banco regenera sus datos, se vuelve a cargar.
    """

    def __init__(self, raiz: Path, capacidad: int = 16):
        self.raiz = raiz
        self.capacidad = capacidad
        self.con = duckdb.connect()
        self.cache: OrderedDict[tuple, tuple[str, list[str], int]] = OrderedDict()
        self.cerrojo = threading.Lock()  # FastAPI atiende peticiones en varios hilos

    def fichero(self, conjunto: str, fecha: date, tabla: str) -> Path:
        return self.raiz / conjunto / f"fecha={fecha.isoformat()}" / f"{TABLES[tabla]}.gz"

    def fechas(self, conjunto: str) -> list[date]:
        carpetas = (self.raiz / conjunto).glob("fecha=*")
        return sorted(date.fromisoformat(p.name.split("=")[1]) for p in carpetas)

    def tabla(self, ruta: Path) -> tuple[str, list[str], int]:
        """Nombre de la tabla en memoria, sus columnas y su número de filas."""
        clave = (str(ruta), ruta.stat().st_mtime_ns)
        with self.cerrojo:
            if clave in self.cache:
                self.cache.move_to_end(clave)
                return self.cache[clave]
            nombre = f"t{len(self.cache)}_{abs(hash(clave))}"
            # all_varchar: se sirve el texto tal cual lo guarda el origen
            self.con.execute(f"""
                CREATE TABLE {nombre} AS SELECT * FROM read_csv(
                    '{ruta.as_posix()}', all_varchar=true, hive_partitioning=false)
            """)
            columnas = [c[0] for c in self.con.execute(f"DESCRIBE {nombre}").fetchall()]
            total = self.con.execute(f"SELECT count(*) FROM {nombre}").fetchone()[0]
            self.cache[clave] = (nombre, columnas, total)
            if len(self.cache) > self.capacidad:  # se descarta la menos usada (LRU)
                _, (viejo, _, _) = self.cache.popitem(last=False)
                self.con.execute(f"DROP TABLE {viejo}")
            return self.cache[clave]

    def pagina(self, nombre: str, desde: int, limite: int) -> list[list[str | None]]:
        cursor = self.con.cursor()  # un cursor por petición: la conexión es compartida
        filas = cursor.execute(f"SELECT * FROM {nombre} LIMIT {limite} OFFSET {desde}").fetchall()
        return [list(fila) for fila in filas]


def codificar_cursor(desde: int) -> str:
    return base64.urlsafe_b64encode(f"desde:{desde}".encode()).decode()


def decodificar_cursor(cursor: str) -> int:
    try:
        prefijo, valor = base64.urlsafe_b64decode(cursor.encode()).decode().split(":")
        assert prefijo == "desde"
        return int(valor)
    except Exception as error:
        raise HTTPException(400, "Cursor no válido") from error


app = FastAPI(
    title="API del sistema origen (simulada)",
    description="Entregas diarias de solicitudes de crédito, paginadas por cursor.",
    version="1.0.0",
)
app.state.almacen = Almacen(ORIGEN_RAIZ)
app.state.tasa_fallos = float(os.getenv("ORIGEN_TASA_FALLOS", "0"))
app.state.azar = random.Random()


@app.exception_handler(FalloSimulado)
def responder_fallo(_: Request, error: FalloSimulado) -> JSONResponse:
    cabeceras = {"Retry-After": "1"} if error.codigo == 429 else {}
    return JSONResponse({"detail": "Fallo simulado"}, status_code=error.codigo, headers=cabeceras)


def autenticar(x_api_key: Annotated[str | None, Header()] = None) -> None:
    if x_api_key != ORIGEN_API_KEY:
        raise HTTPException(401, "Falta la cabecera X-API-Key o no es válida")


def quizas_fallar(request: Request) -> None:
    estado = request.app.state
    if estado.tasa_fallos and estado.azar.random() < estado.tasa_fallos:
        raise FalloSimulado(estado.azar.choice([429, 503]))


@app.get("/salud")
def salud() -> dict:
    return {"estado": "ok"}


@app.get("/v1/{conjunto}/entregas", dependencies=[Depends(autenticar)])
def listar_entregas(conjunto: str, request: Request) -> list[date]:
    fechas = request.app.state.almacen.fechas(conjunto)
    if not fechas:
        raise HTTPException(404, f"No hay entregas del conjunto '{conjunto}'")
    return fechas


@app.get(
    "/v1/{conjunto}/entregas/{fecha}/{tabla}",
    dependencies=[Depends(autenticar), Depends(quizas_fallar)],
)
def leer_pagina(
    conjunto: str,
    fecha: date,
    tabla: str,
    request: Request,
    cursor: str | None = None,
    limite: Annotated[int, Query(ge=1, le=LIMITE_MAXIMO)] = 5_000,
) -> Pagina:
    if tabla not in TABLES:
        raise HTTPException(404, f"Tabla desconocida: '{tabla}'")
    almacen: Almacen = request.app.state.almacen
    ruta = almacen.fichero(conjunto, fecha, tabla)
    if not ruta.exists():
        raise HTTPException(404, f"No hay entrega de '{tabla}' del {fecha} en '{conjunto}'")

    nombre, columnas, total = almacen.tabla(ruta)
    desde = decodificar_cursor(cursor) if cursor else 0
    filas = almacen.pagina(nombre, desde, limite)
    siguiente = codificar_cursor(desde + limite) if desde + limite < total else None
    return Pagina(
        conjunto=conjunto,
        fecha=fecha,
        tabla=tabla,
        total=total,
        columnas=columnas,
        filas=filas,
        siguiente=siguiente,
    )
