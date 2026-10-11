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
cambios, esta comprobación y `destroy` en cada PR.

## Estructura

| Fichero | Contenido |
|---|---|
| `versions.tf` | Versión de Terraform y del proveedor de AWS (`.terraform.lock.hcl` fija la exacta) |
| `providers.tf` | Proveedor de AWS; con `endpoint_aws` apunta a Moto |
| `variables.tf` | Entradas, con validación |
| `main.tf` | Recursos |
| `outputs.tf` | Bucket, URI del lago y ARN del rol |
| `entornos/*.tfvars` | Valores por entorno (`local` = Moto, `dev` = AWS real) |

El estado (`terraform.tfstate`) es local y no se versiona. En un equipo iría en un
*backend* remoto (un bucket S3 con bloqueo) para que todos compartan el mismo.

## Rarezas de Moto (no de nuestro código)

- **Etiquetas al crear el bucket**: el proveedor 6.x las manda en la propia petición
  `CreateBucket` y Moto las ignora; el segundo `apply` las aplica. En AWS real basta uno.
- **Vaciar un bucket con *delete markers*** (lápidas que deja un borrado sin versión)
  hace fallar a Moto y `destroy` se queda reintentando. Por eso la comprobación borra
  la versión concreta de su objeto de prueba, que no deja lápida.

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
