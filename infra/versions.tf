# Versiones de Terraform y de los proveedores (plugins que hablan con cada nube).
# Fijarlas hace la infraestructura reproducible, igual que uv.lock con Python;
# las versiones exactas descargadas quedan en .terraform.lock.hcl (se versiona).
terraform {
  required_version = ">= 1.16"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68" # cualquier 6.x desde la 6.68, nunca la 7
    }
  }
}
