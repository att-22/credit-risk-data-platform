"""Acceso al lago de datos, en disco local o en S3, con la misma interfaz.

Las rutas del lago son cadenas: "E:/.../lake/bronze" o "s3://bucket/bronze".

- Los DATOS los leen y escriben DuckDB (read_csv, read_parquet, COPY), que
  entiende s3:// con su extensión httpfs.
- Las operaciones de CARPETAS (listar, existe, borrar, subir) las hace
  pyarrow.fs, que ofrece la misma API para disco local y para S3.

Credenciales y destino de S3: las variables estándar de AWS (AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION) y AWS_ENDPOINT_URL para
hablar con un emulador como Moto en lugar de con AWS.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import pyarrow.fs as pafs

S3 = "s3://"


def es_s3(ruta: str | Path) -> bool:
    return str(ruta).startswith(S3)


def ruta(r: str | Path) -> str:
    """Normaliza una ruta del lago: URI de S3 tal cual, ruta local absoluta con '/'."""
    return str(r).rstrip("/") if es_s3(r) else Path(r).resolve().as_posix()


def unir(base: str | Path, *partes: str) -> str:
    return "/".join([ruta(base), *partes])


def nombre(r: str | Path) -> str:
    """Último tramo de la ruta (como Path.name)."""
    return ruta(r).rsplit("/", 1)[-1]


def _region() -> str:
    return os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "eu-west-1"


def _sistema(r: str | Path) -> tuple[pafs.FileSystem, str]:
    """Sistema de ficheros de pyarrow y ruta dentro de él."""
    r = ruta(r)
    if not es_s3(r):
        return pafs.LocalFileSystem(), r
    # Sin claves en el entorno, pyarrow usa la cadena de credenciales de AWS (rol, SSO...)
    return pafs.S3FileSystem(
        region=_region(),
        endpoint_override=os.getenv("AWS_ENDPOINT_URL"),
        # La red falla de vez en cuando (en la nube también): reintentar con espera
        retry_strategy=pafs.AwsDefaultS3RetryStrategy(max_attempts=8),
    ), r.removeprefix(S3)


def existe(r: str | Path) -> bool:
    fs, p = _sistema(r)
    return fs.get_file_info(p).type != pafs.FileType.NotFound


def subcarpetas(r: str | Path, prefijo: str = "") -> list[str]:
    """Carpetas directamente dentro de r, con algún fichero, cuyo nombre empieza por
    prefijo, ordenadas.

    En S3 no hay carpetas de verdad: son prefijos de las claves de los objetos. Se
    deducen de los ficheros (un listado recursivo) y no de los "prefijos comunes"
    que devuelve S3, porque Moto incluye entre ellos los de objetos ya borrados en
    un bucket versionado.
    """
    fs, p = _sistema(r)
    selector = pafs.FileSelector(p, allow_not_found=True, recursive=True)
    nombres = {
        i.path.removeprefix(p + "/").split("/", 1)[0]
        for i in fs.get_file_info(selector)
        if i.type == pafs.FileType.File and "/" in i.path.removeprefix(p + "/")
    }
    return sorted(unir(r, n) for n in nombres if n.startswith(prefijo))


def ficheros(r: str | Path) -> set[str]:
    """Nombres de los ficheros directamente dentro de r."""
    fs, p = _sistema(r)
    selector = pafs.FileSelector(p, allow_not_found=True)
    return {i.base_name for i in fs.get_file_info(selector) if i.type == pafs.FileType.File}


def borrar(r: str | Path) -> None:
    """Borra una carpeta con todo su contenido, si existe."""
    fs, p = _sistema(r)
    if fs.get_file_info(p).type != pafs.FileType.Directory:
        return
    # delete_dir borra también el "marcador de directorio" (objeto vacío "carpeta/")
    # que pyarrow deja en S3 al vaciar una carpeta; sin eso seguiría apareciendo.
    try:
        fs.delete_dir(p)
    except FileNotFoundError:
        pass  # Moto puede dar por existente una carpeta de objetos ya borrados


def subir(origen: Path, destino: str) -> None:
    """Copia una carpeta local al lago (S3 o disco), sustituyendo lo que hubiera."""
    borrar(destino)
    fs, p = _sistema(destino)
    pafs.copy_files(origen.resolve().as_posix(), p, pafs.LocalFileSystem(), fs)


def conectar(*rutas: str | Path) -> duckdb.DuckDBPyConnection:
    """Conexión de DuckDB en memoria, preparada para S3 si alguna ruta lo usa."""
    con = duckdb.connect()
    if any(es_s3(r) for r in rutas):
        configurar_s3(con)
    return con


def configurar_s3(con: duckdb.DuckDBPyConnection) -> None:
    """Carga httpfs y crea el secreto de S3 a partir de las variables de AWS."""
    con.execute("INSTALL httpfs; LOAD httpfs")
    opciones = {"REGION": _region()}
    if os.getenv("AWS_ACCESS_KEY_ID"):
        opciones |= {
            "KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "SECRET": os.environ["AWS_SECRET_ACCESS_KEY"],
            "SESSION_TOKEN": os.getenv("AWS_SESSION_TOKEN", ""),
        }
    else:
        opciones["PROVIDER"] = "credential_chain"
    if endpoint := os.getenv("AWS_ENDPOINT_URL"):
        url = urlparse(endpoint)
        # Un emulador no tiene un DNS por bucket: rutas tipo host/bucket/clave
        opciones |= {
            "ENDPOINT": url.netloc,
            "USE_SSL": str(url.scheme == "https").lower(),
            "URL_STYLE": "path",
        }
    lista = ", ".join(
        f"{k} {v}" if k in ("PROVIDER", "USE_SSL") else f"{k} '{v}'" for k, v in opciones.items()
    )
    con.execute(f"CREATE OR REPLACE SECRET lago (TYPE s3, {lista})")


def copiar(con: duckdb.DuckDBPyConnection, consulta: str, destino: str, opciones: str) -> None:
    """COPY (consulta) TO destino sin dejar nunca un fichero a medias.

    En disco se escribe un temporal y se renombra (el renombrado es atómico). En S3
    no hay renombrado, pero no hace falta: un objeto solo aparece cuando la subida
    ha terminado entera.
    """
    if es_s3(destino):
        con.execute(f"COPY ({consulta}) TO '{destino}' ({opciones})")
        return
    final = Path(destino)
    final.parent.mkdir(parents=True, exist_ok=True)
    temporal = final.with_name(final.name + ".tmp")
    con.execute(f"COPY ({consulta}) TO '{temporal.as_posix()}' ({opciones})")
    temporal.replace(final)
