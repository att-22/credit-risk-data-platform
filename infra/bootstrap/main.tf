# Arranque del despliegue continuo: lo que tiene que existir ANTES de que GitHub
# Actions pueda desplegar. Lo aplica una persona, una sola vez por cuenta de AWS:
#   - el bucket donde se guarda el estado de Terraform de infra/,
#   - el proveedor OIDC de GitHub (AWS se fía de los tokens que emite GitHub),
#   - el rol que asume el workflow de despliegue, sin claves guardadas en GitHub.
# Su propio estado es local: es pequeño, cambia muy poco y no puede guardarse en un
# bucket que todavía no existe.

terraform {
  required_version = ">= 1.16"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      proyecto   = var.proyecto
      gestionado = "terraform-bootstrap"
    }
  }

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

data "aws_caller_identity" "actual" {}

locals {
  cuenta        = data.aws_caller_identity.actual.account_id
  bucket_estado = "${var.proyecto}-tfstate-${local.cuenta}"
  emisor        = "token.actions.githubusercontent.com"
}

# --- Estado remoto de Terraform --------------------------------------------------

# El estado dice qué recursos ha creado Terraform. En un equipo (o en la CI) no puede
# vivir en el disco de nadie: va a un bucket, con versionado para poder recuperarlo.
resource "aws_s3_bucket" "estado" {
  bucket = local.bucket_estado
}

resource "aws_s3_bucket_versioning" "estado" {
  bucket = aws_s3_bucket.estado.id
  versioning_configuration {
    status = "Enabled"
  }
}

# El estado puede contener datos sensibles: cifrado y nunca público
resource "aws_s3_bucket_server_side_encryption_configuration" "estado" {
  bucket = aws_s3_bucket.estado.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "estado" {
  bucket                  = aws_s3_bucket.estado.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Confianza en GitHub (OIDC) --------------------------------------------------

# OIDC: en cada ejecución, GitHub firma un token que dice "soy el repositorio X, en
# el environment Y". AWS lo valida y entrega credenciales que caducan en una hora.
# No hay ninguna clave de AWS guardada en GitHub que se pueda filtrar.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://${local.emisor}"
  client_id_list = ["sts.amazonaws.com"]
}

# Quién puede asumir el rol: SOLO tokens de GitHub, de ESTE repositorio y de un job
# que declare el environment de despliegue. Sin la condición "sub", cualquier
# repositorio de GitHub del mundo podría asumirlo.
data "aws_iam_policy_document" "confianza" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.emisor}:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.emisor}:sub"
      values   = ["repo:${var.repositorio}:environment:${var.entorno_github}"]
    }
  }
}

resource "aws_iam_role" "despliegue" {
  name               = "${var.proyecto}-despliegue"
  assume_role_policy = data.aws_iam_policy_document.confianza.json
}

# Qué puede hacer el rol: lo justo para aplicar infra/ (el lago y el rol del
# pipeline) y para leer y escribir su estado. Nada fuera del prefijo del proyecto.
data "aws_iam_policy_document" "despliegue" {
  statement {
    sid       = "EstadoListar"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.estado.arn]
  }
  statement {
    sid       = "EstadoLeerYEscribir"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.estado.arn}/*"]
  }
  statement {
    sid     = "GestionarElLago"
    actions = ["s3:*"]
    # El bucket y sus objetos (vaciarlo en un destroy, la comprobación tras el apply)
    resources = [
      "arn:aws:s3:::${var.proyecto}-*-lake-${local.cuenta}",
      "arn:aws:s3:::${var.proyecto}-*-lake-${local.cuenta}/*",
    ]
  }
  statement {
    sid = "GestionarRolYPoliticaDelPipeline"
    actions = [
      "iam:GetRole", "iam:CreateRole", "iam:DeleteRole", "iam:UpdateAssumeRolePolicy",
      "iam:TagRole", "iam:UntagRole", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
      "iam:ListInstanceProfilesForRole", "iam:AttachRolePolicy", "iam:DetachRolePolicy",
      "iam:GetPolicy", "iam:CreatePolicy", "iam:DeletePolicy", "iam:TagPolicy", "iam:UntagPolicy",
      "iam:GetPolicyVersion", "iam:CreatePolicyVersion", "iam:DeletePolicyVersion",
      "iam:ListPolicyVersions",
    ]
    resources = [
      "arn:aws:iam::${local.cuenta}:role/${var.proyecto}-*-pipeline",
      "arn:aws:iam::${local.cuenta}:policy/${var.proyecto}-*-acceso-lago",
    ]
  }
}

resource "aws_iam_policy" "despliegue" {
  name   = "${var.proyecto}-despliegue"
  policy = data.aws_iam_policy_document.despliegue.json
}

resource "aws_iam_role_policy_attachment" "despliegue" {
  role       = aws_iam_role.despliegue.name
  policy_arn = aws_iam_policy.despliegue.arn
}
