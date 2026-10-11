"""Comprueba que la infraestructura creada por Terraform es la que se declaró.

No se fía de Terraform: pregunta directamente a la API de AWS (o a Moto) con boto3
y falla si algo no cumple lo que promete infra/main.tf:
  1. El bucket del lago existe, con versionado, cifrado y sin acceso público.
  2. La landing caduca a los N días y las versiones antiguas a los 30.
  3. El rol del pipeline tiene una sola política, de mínimo privilegio: solo
     listar el bucket y leer/escribir/borrar objetos dentro de las zonas del lago.
  4. Un objeto escrito en bronze/ se puede leer de vuelta (el lago funciona).

Uso (con Moto arrancado y `terraform apply` hecho):
    uv run --group aws python scripts/comprobar_infra.py
    uv run --group aws python scripts/comprobar_infra.py --entorno dev   # AWS real
"""

from __future__ import annotations

import argparse
import sys

import boto3

ACCIONES_OBJETOS = {"s3:GetObject", "s3:PutObject", "s3:DeleteObject"}
ZONAS = {"landing", "origen", "bronze", "warehouse"}


def comprobar(entorno: str, endpoint: str | None, region: str, dias_landing: int) -> list[str]:
    """Devuelve la lista de fallos (vacía si todo cumple)."""
    # Con emulador vale cualquier credencial; con AWS real se usan las del entorno
    falsas = {"aws_access_key_id": "test", "aws_secret_access_key": "test"} if endpoint else {}
    sesion = boto3.Session(region_name=region, **falsas)
    s3 = sesion.client("s3", endpoint_url=endpoint)
    iam = sesion.client("iam", endpoint_url=endpoint)
    cuenta = sesion.client("sts", endpoint_url=endpoint).get_caller_identity()["Account"]

    bucket = f"credit-risk-{entorno}-lake-{cuenta}"
    fallos: list[str] = []

    def exigir(condicion: bool, mensaje: str) -> None:
        print(("  ok   " if condicion else "  FALLO ") + mensaje)
        if not condicion:
            fallos.append(mensaje)

    print(f"Bucket {bucket}")
    exigir(
        s3.get_bucket_versioning(Bucket=bucket).get("Status") == "Enabled",
        "versionado activado",
    )
    reglas_cifrado = s3.get_bucket_encryption(Bucket=bucket)["ServerSideEncryptionConfiguration"][
        "Rules"
    ]
    exigir(
        any(
            r["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
            for r in reglas_cifrado
        ),
        "cifrado en reposo AES256",
    )
    bloqueo = s3.get_public_access_block(Bucket=bucket)["PublicAccessBlockConfiguration"]
    exigir(all(bloqueo.values()) and len(bloqueo) == 4, "acceso público bloqueado (4/4)")

    reglas = {
        r["ID"]: r
        for r in s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
        if r["Status"] == "Enabled"
    }
    landing = reglas.get("caducar-landing", {})
    exigir(
        landing.get("Filter", {}).get("Prefix") == "landing/"
        and landing.get("Expiration", {}).get("Days") == dias_landing,
        f"landing/ caduca a los {dias_landing} días",
    )
    exigir(
        reglas.get("caducar-versiones-antiguas", {})
        .get("NoncurrentVersionExpiration", {})
        .get("NoncurrentDays")
        == 30,
        "versiones antiguas caducan a los 30 días",
    )

    rol = f"credit-risk-{entorno}-pipeline"
    print(f"Rol {rol}")
    adjuntas = iam.list_attached_role_policies(RoleName=rol)["AttachedPolicies"]
    en_linea = iam.list_role_policies(RoleName=rol)["PolicyNames"]
    exigir(len(adjuntas) == 1 and not en_linea, "una única política, sin políticas en línea")
    if adjuntas:
        arn = adjuntas[0]["PolicyArn"]
        version = iam.get_policy(PolicyArn=arn)["Policy"]["DefaultVersionId"]
        documento = iam.get_policy_version(PolicyArn=arn, VersionId=version)["PolicyVersion"][
            "Document"
        ]
        acciones, recursos = set(), set()
        for sentencia in documento["Statement"]:
            exigir(sentencia["Effect"] == "Allow", f"{sentencia.get('Sid')}: solo Allow")
            acciones |= set(_lista(sentencia["Action"]))
            recursos |= set(_lista(sentencia["Resource"]))
        exigir(
            acciones == ACCIONES_OBJETOS | {"s3:ListBucket"},
            "acciones exactas: ListBucket + Get/Put/DeleteObject (ni comodines ni más)",
        )
        esperados = {f"arn:aws:s3:::{bucket}"} | {
            f"arn:aws:s3:::{bucket}/{zona}/*" for zona in ZONAS
        }
        exigir(recursos == esperados, "recursos: el bucket y sus 4 zonas, nada más")

    print("Lago")
    clave = "bronze/_comprobacion/hola.txt"
    version = s3.put_object(Bucket=bucket, Key=clave, Body=b"hola lago")["VersionId"]
    leido = s3.get_object(Bucket=bucket, Key=clave)["Body"].read()
    # Borrar la versión concreta es definitivo y no deja "delete marker" (lápida)
    s3.delete_object(Bucket=bucket, Key=clave, VersionId=version)
    exigir(leido == b"hola lago", "escribir y leer un objeto en bronze/")

    return fallos


def _lista(valor: str | list[str]) -> list[str]:
    return [valor] if isinstance(valor, str) else valor


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
    args = parser.parse_args()

    fallos = comprobar(args.entorno, args.endpoint or None, args.region, args.dias_landing)
    if fallos:
        sys.exit(f"\n{len(fallos)} comprobación(es) fallida(s)")
    print("\nOK: la infraestructura cumple lo declarado")


if __name__ == "__main__":
    main()
