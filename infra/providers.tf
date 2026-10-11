# Proveedor de AWS. Con var.endpoint_aws apunta a un emulador (Moto) en lugar de a
# AWS: las mismas definiciones sirven para practicar gratis y para la nube real.

provider "aws" {
  region = var.region

  # Etiquetas en TODOS los recursos: en la nube real sirven para saber de quién es
  # cada cosa y repartir los costes.
  default_tags {
    tags = {
      proyecto   = var.proyecto
      entorno    = var.entorno
      gestionado = "terraform"
    }
  }

  # Solo con emulador: no validar credenciales reales ni consultar AWS, y usar
  # rutas tipo http://emulador/bucket (el emulador no tiene DNS por bucket).
  skip_credentials_validation = var.endpoint_aws != null
  skip_requesting_account_id  = var.endpoint_aws != null
  skip_metadata_api_check     = var.endpoint_aws != null
  s3_use_path_style           = var.endpoint_aws != null

  dynamic "endpoints" {
    for_each = var.endpoint_aws == null ? [] : [var.endpoint_aws]
    content {
      s3  = endpoints.value
      iam = endpoints.value
      sts = endpoints.value
    }
  }
}
