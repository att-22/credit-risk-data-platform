"""Comprueba que la infraestructura creada por Terraform es la que se declaró.

No se fía de Terraform: pregunta directamente a la API de AWS (o a Moto) con boto3
y falla si algo no cumple lo que promete infra/main.tf:
  1. El bucket del lago existe, con versionado, cifrado y sin acceso público.
  2. La landing caduca a los N días y las versiones antiguas a los 30.
  3. El rol del pipeline tiene una sola política, de mínimo privilegio: solo
     listar el bucket y leer/escribir/borrar objetos dentro de las zonas del lago.
  4. Un objeto escrito en bronze/ se puede leer de vuelta (el lago funciona).

Con --bootstrap comprueba en su lugar lo que crea infra/bootstrap/ para el
despliegue continuo: el bucket del estado y, sobre todo, que el rol de despliegue
solo lo puede asumir GitHub Actions desde este repositorio y su environment.

Uso (con Moto arrancado y `terraform apply` hecho):
    uv run --group aws python scripts/comprobar_infra.py
    uv run --group aws python scripts/comprobar_infra.py --bootstrap
    uv run --group aws python scripts/comprobar_infra.py --entorno dev --endpoint ""  # AWS real
"""

from __future__ import annotations

import argparse
import sys

import boto3

ACCIONES_OBJETOS = {"s3:GetObject", "s3:PutObject", "s3:DeleteObject"}
ZONAS = {"landing", "origen", "bronze", "warehouse"}
EMISOR_GITHUB = "token.actions.githubusercontent.com"
# Acciones con las que una identidad podría darse más permisos de los que tiene
ACCIONES_PELIGROSAS = {"*", "iam:*", "iam:PassRole", "iam:CreateUser", "iam:CreateAccessKey"}


class Comprobacion:
    """Clientes de AWS (o del emulador) y la lista de fallos encontrados."""

    def __init__(self, endpoint: str | None, region: str) -> None:
        # Con emulador vale cualquier credencial; con AWS real se usan las del entorno
        falsas = {"aws_access_key_id": "test", "aws_secret_access_key": "test"} if endpoint else {}
        sesion = boto3.Session(region_name=region, **falsas)
        self.s3 = sesion.client("s3", endpoint_url=endpoint)
        self.iam = sesion.client("iam", endpoint_url=endpoint)
        self.cuenta = sesion.client("sts", endpoint_url=endpoint).get_caller_identity()["Account"]
        self.fallos: list[str] = []

    def exigir(self, condicion: bool, mensaje: str) -> None:
        print(("  ok   " if condicion else "  FALLO ") + mensaje)
        if not condicion:
            self.fallos.append(mensaje)

    def bucket_protegido(self, bucket: str) -> None:
        """Versionado, cifrado en reposo y nada público."""
        print(f"Bucket {bucket}")
        self.exigir(
            self.s3.get_bucket_versioning(Bucket=bucket).get("Status") == "Enabled",
            "versionado activado",
        )
        cifrado = self.s3.get_bucket_encryption(Bucket=bucket)["ServerSideEncryptionConfiguration"]
        self.exigir(
            any(
                r["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
                for r in cifrado["Rules"]
            ),
            "cifrado en reposo AES256",
        )
        bloqueo = self.s3.get_public_access_block(Bucket=bucket)["PublicAccessBlockConfiguration"]
        self.exigir(all(bloqueo.values()) and len(bloqueo) == 4, "acceso público bloqueado (4/4)")

    def politica_del_rol(self, rol: str) -> list[dict]:
        """Sentencias de la única política adjunta al rol ([] si no es única)."""
        adjuntas = self.iam.list_attached_role_policies(RoleName=rol)["AttachedPolicies"]
        en_linea = self.iam.list_role_policies(RoleName=rol)["PolicyNames"]
        self.exigir(
            len(adjuntas) == 1 and not en_linea, "una única política, sin políticas en línea"
        )
        if len(adjuntas) != 1:
            return []
        arn = adjuntas[0]["PolicyArn"]
        version = self.iam.get_policy(PolicyArn=arn)["Policy"]["DefaultVersionId"]
        documento = self.iam.get_policy_version(PolicyArn=arn, VersionId=version)
        sentencias = documento["PolicyVersion"]["Document"]["Statement"]
        for sentencia in sentencias:
            self.exigir(sentencia["Effect"] == "Allow", f"{sentencia.get('Sid')}: solo Allow")
        return sentencias


def comprobar(entorno: str, endpoint: str | None, region: str, dias_landing: int) -> list[str]:
    """Infraestructura de infra/: devuelve la lista de fallos (vacía si todo cumple)."""
    c = Comprobacion(endpoint, region)
    bucket = f"credit-risk-{entorno}-lake-{c.cuenta}"
    c.bucket_protegido(bucket)

    reglas = {
        r["ID"]: r
        for r in c.s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
        if r["Status"] == "Enabled"
    }
    landing = reglas.get("caducar-landing", {})
    c.exigir(
        landing.get("Filter", {}).get("Prefix") == "landing/"
        and landing.get("Expiration", {}).get("Days") == dias_landing,
        f"landing/ caduca a los {dias_landing} días",
    )
    c.exigir(
        reglas.get("caducar-versiones-antiguas", {})
        .get("NoncurrentVersionExpiration", {})
        .get("NoncurrentDays")
        == 30,
        "versiones antiguas caducan a los 30 días",
    )

    rol = f"credit-risk-{entorno}-pipeline"
    print(f"Rol {rol}")
    sentencias = c.politica_del_rol(rol)
    if sentencias:
        c.exigir(
            _acciones(sentencias) == ACCIONES_OBJETOS | {"s3:ListBucket"},
            "acciones exactas: ListBucket + Get/Put/DeleteObject (ni comodines ni más)",
        )
        esperados = {f"arn:aws:s3:::{bucket}"} | {
            f"arn:aws:s3:::{bucket}/{zona}/*" for zona in ZONAS
        }
        c.exigir(_recursos(sentencias) == esperados, "recursos: el bucket y sus 4 zonas, nada más")

    print("Lago")
    clave = "bronze/_comprobacion/hola.txt"
    version = c.s3.put_object(Bucket=bucket, Key=clave, Body=b"hola lago")["VersionId"]
    leido = c.s3.get_object(Bucket=bucket, Key=clave)["Body"].read()
    # Borrar la versión concreta es definitivo y no deja "delete marker" (lápida)
    c.s3.delete_object(Bucket=bucket, Key=clave, VersionId=version)
    c.exigir(leido == b"hola lago", "escribir y leer un objeto en bronze/")

    return c.fallos


def comprobar_bootstrap(
    endpoint: str | None, region: str, repositorio: str, entorno_github: str
) -> list[str]:
    """Lo que crea infra/bootstrap/ para el despliegue continuo."""
    c = Comprobacion(endpoint, region)
    c.bucket_protegido(f"credit-risk-tfstate-{c.cuenta}")

    proveedor = f"arn:aws:iam::{c.cuenta}:oidc-provider/{EMISOR_GITHUB}"
    print("Proveedor OIDC de GitHub")
    oidc = c.iam.get_open_id_connect_provider(OpenIDConnectProviderArn=proveedor)
    c.exigir(oidc["ClientIDList"] == ["sts.amazonaws.com"], "única audiencia: sts.amazonaws.com")

    rol = "credit-risk-despliegue"
    print(f"Rol {rol}: quién puede asumirlo")
    confianza = c.iam.get_role(RoleName=rol)["Role"]["AssumeRolePolicyDocument"]["Statement"]
    c.exigir(len(confianza) == 1, "una única sentencia de confianza")
    sentencia = confianza[0]
    c.exigir(
        sentencia.get("Principal") == {"Federated": proveedor},
        "solo el proveedor OIDC de GitHub (ni usuarios ni otras cuentas)",
    )
    c.exigir(
        _lista(sentencia["Action"]) == ["sts:AssumeRoleWithWebIdentity"],
        "solo sts:AssumeRoleWithWebIdentity",
    )
    condicion = sentencia.get("Condition", {})
    # StringEquals y no StringLike: sin comodines que abran la puerta a otros repositorios
    c.exigir(set(condicion) == {"StringEquals"}, "condiciones solo de igualdad exacta")
    exactas = condicion.get("StringEquals", {})
    c.exigir(
        _lista(exactas.get(f"{EMISOR_GITHUB}:sub", []))
        == [f"repo:{repositorio}:environment:{entorno_github}"],
        f"solo el repositorio {repositorio} desde el environment {entorno_github}",
    )
    c.exigir(
        _lista(exactas.get(f"{EMISOR_GITHUB}:aud", [])) == ["sts.amazonaws.com"],
        "token emitido para AWS (aud)",
    )

    print(f"Rol {rol}: qué puede hacer")
    sentencias = c.politica_del_rol(rol)
    recursos = _recursos(sentencias)
    c.exigir(
        bool(recursos) and all(":credit-risk-" in r or "/credit-risk-" in r for r in recursos),
        "todos los recursos son del proyecto (ningún '*')",
    )
    c.exigir(
        not _acciones(sentencias) & ACCIONES_PELIGROSAS,
        "sin iam:*, PassRole ni creación de usuarios o claves",
    )
    de_iam = [s for s in sentencias if any(a.startswith("iam:") for a in _lista(s["Action"]))]
    c.exigir(
        all(":role/credit-risk-" in r or ":policy/credit-risk-" in r for r in _recursos(de_iam)),
        "IAM limitado a roles y políticas del proyecto",
    )
    return c.fallos


def _lista(valor: str | list[str]) -> list[str]:
    return [valor] if isinstance(valor, str) else valor


def _acciones(sentencias: list[dict]) -> set[str]:
    return {a for s in sentencias for a in _lista(s["Action"])}


def _recursos(sentencias: list[dict]) -> set[str]:
    return {r for s in sentencias for r in _lista(s["Resource"])}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--entorno", default="local")
    parser.add_argument(
        "--endpoint",
        default="http://localhost:5000",
        help="URL del emulador; vacío ('') para AWS real",
    )
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--dias-landing", type=int, default=30)
    parser.add_argument("--bootstrap", action="store_true", help="comprobar infra/bootstrap/")
    parser.add_argument("--repositorio", default="att-22/credit-risk-data-platform")
    parser.add_argument("--entorno-github", default="aws-dev")
    args = parser.parse_args()

    endpoint = args.endpoint or None
    if args.bootstrap:
        fallos = comprobar_bootstrap(endpoint, args.region, args.repositorio, args.entorno_github)
    else:
        fallos = comprobar(args.entorno, endpoint, args.region, args.dias_landing)
    if fallos:
        sys.exit(f"\n{len(fallos)} comprobación(es) fallida(s)")
    print("\nOK: la infraestructura cumple lo declarado")


if __name__ == "__main__":
    main()
