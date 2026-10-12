#!/usr/bin/env sh
# Genera infra/backend_override.tf: le dice a Terraform que guarde el estado en un
# bucket S3 (estado remoto) en lugar de en un fichero local.
#
# infra/ no lleva ningún backend escrito, para que en local baste con `terraform
# init`. Terraform fusiona los ficheros *_override.tf con el resto, así que este
# fichero (que no se versiona) activa el estado remoto solo donde se genera: en la
# CI y en el despliegue.
#
# Uso: sh infra/backend.sh <bucket> <entorno> [endpoint_del_emulador]
set -eu

bucket=$1
entorno=$2
endpoint=${3:-}
destino="$(dirname "$0")/backend_override.tf"

{
  echo 'terraform {'
  echo '  backend "s3" {'
  echo "    bucket = \"$bucket\""
  echo "    key    = \"$entorno/terraform.tfstate\"  # un estado por entorno"
  echo "    region = \"${AWS_REGION:-eu-west-1}\""
  # Bloqueo: mientras alguien aplica, un fichero .tflock impide que otro lo haga a la
  # vez y se corrompa el estado (S3 lo garantiza con escrituras condicionales)
  echo '    use_lockfile = true'
  if [ -n "$endpoint" ]; then
    echo "    endpoints                   = { s3 = \"$endpoint\" }"
    echo '    use_path_style              = true'
    echo '    skip_credentials_validation = true'
    echo '    skip_requesting_account_id  = true'
    echo '    skip_metadata_api_check     = true'
    echo '    skip_region_validation      = true'
  fi
  echo '  }'
  echo '}'
} > "$destino"

echo "Estado remoto: s3://$bucket/$entorno/terraform.tfstate"
