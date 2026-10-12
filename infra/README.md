# Infraestructura como código (Terraform + AWS)

La infraestructura de la plataforma en AWS, declarada con Terraform:

| Recurso | Para qué |
|---|---|
| Bucket S3 `credit-risk-<entorno>-lake-<cuenta>` | El lago de datos (`landing/`, `origen/`, `bronze/`, `warehouse/`) |
| Versionado, cifrado AES256, bloqueo de acceso público | Protección de los datos: deshacer errores, cifrado en reposo, nada público |
| Ciclo de vida | `landing/` caduca a los 30 días (7 en dev); versiones antiguas, a los 30 |
| Rol IAM `credit-risk-<entorno>-pipeline` + política | Mínimo privilegio: listar el bucket y leer/escribir/borrar solo en sus 4 zonas |

Sin cuenta de AWS ni coste: en local y en la CI se despliega contra
[Moto](https://github.com/getmoto/moto), un emulador de AWS de código abierto. Lo
único que cambia respecto a AWS real es la variable `endpoint_aws`.

## Uso

Terraform y Moto se ejecutan en contenedores (perfil `aws` de `compose.yaml`); Moto
publica el puerto **5000**.

```bash
docker compose --profile aws up -d moto
docker compose --profile aws run --rm terraform init
docker compose --profile aws run --rm terraform plan  -var-file=entornos/local.tfvars
docker compose --profile aws run --rm terraform apply -var-file=entornos/local.tfvars
uv run --group aws python scripts/comprobar_infra.py   # verificación independiente con boto3
docker compose --profile aws run --rm terraform destroy -var-file=entornos/local.tfvars
docker compose --profile aws down
```

`scripts/comprobar_infra.py` no se fía de Terraform: consulta la API (de Moto o de
AWS) y falla si el bucket, el ciclo de vida o los permisos del rol no son los
declarados. La CI (job *Terraform*) hace `fmt`, `validate`, `apply`, `plan` sin
cambios, esta comprobación y `destroy` en cada PR, con el estado en un bucket.

## El lago en S3

Con `LAKE_URI` la landing y bronze viven en el bucket en lugar de en disco; el resto
del pipeline no cambia. Las credenciales y el destino son las variables estándar de AWS:

```bash
export LAKE_URI=s3://credit-risk-local-lake-123456789012   # salida uri_lago de Terraform
export AWS_ENDPOINT_URL=http://localhost:5000 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=eu-west-1
export DBT_TARGET=s3                                       # target de dbt con httpfs y el secreto de S3

uv run simulate-source     # sube las entregas a s3://.../landing/
uv run ingest-bronze       # s3://.../landing/ -> s3://.../bronze/ (Parquet particionado)
cd transform && uv run dbt build

uv run python scripts/comprobar_paridad_s3.py --lago $LAKE_URI   # mismo resultado que en disco
```

Cómo está hecho ([`src/credit_risk/lago.py`](../src/credit_risk/lago.py)):

- Los **datos** los lee y escribe DuckDB (`read_csv`, `read_parquet`, `COPY`), que
  entiende `s3://` con su extensión `httpfs`.
- Las **carpetas** (listar, existe, borrar, subir) las gestiona `pyarrow.fs`, con la
  misma API para disco y S3.
- En disco, escribir sin dejar ficheros a medias exige un temporal y un renombrado;
  en S3 no hay renombrado ni hace falta: un objeto solo aparece cuando su subida termina.
- El almacén de dbt (`warehouse.duckdb`) sigue en `LAKE_DIR`, en local: es el estado
  del motor de cálculo, no parte del lago. Almacenamiento y cómputo quedan separados.

`comprobar_paridad_s3.py` ejecuta el pipeline entero dos veces, con el lago en disco
y en S3, y exige el mismo bronze (240 ficheros, mismas filas) y el mismo mart, columna
a columna. La CI (job *Lago en S3*) lo comprueba en cada PR junto con la paridad de
la carga incremental sobre S3. Streaming, CDC, Spark y Airflow siguen usando el lago local.

## Despliegue continuo

[`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml) aplica `infra/` en
AWS real cada vez que un cambio de la infraestructura llega a `main` (y a mano, desde
la pestaña *Actions*, para ver un `plan` sin aplicar nada). Está **desactivado**: el
job se omite mientras no exista la variable de repositorio `AWS_DEPLOY_ROLE_ARN`.

- **Sin claves guardadas (OIDC).** En cada ejecución GitHub firma un token que dice
  "soy este repositorio, en el environment `aws-dev`". AWS lo valida contra la
  política de confianza del rol y entrega credenciales que caducan en una hora.
- **Estado remoto.** El estado de Terraform se guarda en un bucket S3 con bloqueo
  (`use_lockfile`): dos despliegues a la vez no pueden corromperlo.
  [`backend.sh`](backend.sh) genera esa configuración; `infra/` no la lleva escrita
  para que en local baste con `terraform init`.
- **Se aplica el plan calculado** (`plan -out` y `apply` de ese fichero), y después
  `comprobar_infra.py` verifica los recursos en AWS.

[`bootstrap/`](bootstrap) crea lo que tiene que existir antes: el bucket del estado,
el proveedor OIDC de GitHub y el rol de despliegue, que solo se deja asumir desde
este repositorio y el environment `aws-dev`, y solo puede gestionar los recursos del
proyecto. Lo aplica una persona, una vez por cuenta. La CI ensaya contra Moto los
mismos pasos que el despliegue (bootstrap, estado remoto, plan guardado y `apply`).

Para activarlo con una cuenta de AWS:

```bash
# 1. Con credenciales de administrador de la cuenta, una sola vez:
terraform -chdir=infra/bootstrap init
terraform -chdir=infra/bootstrap apply

# 2. Copiar las dos salidas a las variables del repositorio:
gh variable set TF_STATE_BUCKET     --body "$(terraform -chdir=infra/bootstrap output -raw bucket_estado)"
gh variable set AWS_DEPLOY_ROLE_ARN --body "$(terraform -chdir=infra/bootstrap output -raw rol_despliegue)"
```

3. En GitHub, *Settings → Environments → aws-dev*: añadir un revisor obligatorio
   (la aprobación manual antes de cada `apply`).

Moto no aplica los permisos de IAM, así que la política del rol de despliegue está
comprobada en su forma (sin comodines, solo recursos del proyecto) pero no ejecutada
contra AWS: puede necesitar algún permiso más la primera vez.

## Estructura

| Fichero | Contenido |
|---|---|
| `versions.tf` | Versión de Terraform y del proveedor de AWS (`.terraform.lock.hcl` fija la exacta) |
| `providers.tf` | Proveedor de AWS; con `endpoint_aws` apunta a Moto |
| `variables.tf` | Entradas, con validación |
| `main.tf` | Recursos |
| `outputs.tf` | Bucket, URI del lago y ARN del rol |
| `entornos/*.tfvars` | Valores por entorno (`local` = Moto, `dev` = AWS real) |
| `backend.sh` | Genera la configuración del estado remoto en S3 |
| `bootstrap/` | Bucket del estado, proveedor OIDC y rol de despliegue |

En local el estado (`terraform.tfstate`) es un fichero que no se versiona. En la CI
y en el despliegue va a un *backend* remoto (un bucket S3 con bloqueo).

## Rarezas de Moto (no de nuestro código)

- **Etiquetas al crear el bucket**: el proveedor 6.x las manda en la propia petición
  `CreateBucket` y Moto las ignora; el segundo `apply` las aplica. En AWS real basta uno.
- **Vaciar un bucket con *delete markers*** (lápidas que deja un borrado sin versión)
  hace fallar a Moto y `destroy` se queda reintentando. Por eso la comprobación borra
  la versión concreta de su objeto de prueba, que no deja lápida.

- **"Carpetas" de objetos borrados**: en un bucket versionado, Moto sigue devolviendo
  como prefijo común (carpeta) el de objetos que ya solo son una lápida. Por eso
  `lago.subcarpetas` deduce las carpetas de los ficheros vivos, que es correcto en
  ambos, y el job *Lago en S3* no hace `destroy` (toparía con la rareza anterior).

El ciclo de vida tarda ~1 min en crearse también en AWS: el proveedor espera a leer
la misma regla 10 veces seguidas porque los cambios tardan en propagarse.

## Equivalencias entre nubes

| Concepto | AWS | Google Cloud | Azure |
|---|---|---|---|
| Almacenamiento de objetos (el lago) | S3 | Cloud Storage | Blob Storage / ADLS Gen2 |
| Identidad de una aplicación | IAM role | Service account | Managed identity |
| Permisos | IAM policy | IAM role binding | Role assignment (RBAC) |
| Acceso desde GitHub Actions sin claves | OIDC + `AssumeRoleWithWebIdentity` | Workload Identity Federation | Federated credentials |
| Orquestador gestionado (Airflow) | MWAA | Cloud Composer | Data Factory (Airflow) |
| Spark gestionado | EMR / Glue | Dataproc | Synapse / Databricks |
| Almacén analítico | Redshift / Athena | BigQuery | Synapse / Fabric |
| Kafka gestionado | MSK | Managed Kafka | Event Hubs |
| Proveedor de Terraform | `hashicorp/aws` | `hashicorp/google` | `hashicorp/azurerm` |

Terraform es el mismo en las tres; cambia el proveedor y el nombre de los recursos.
