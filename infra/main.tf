# Infraestructura de la plataforma en AWS:
#   - un bucket S3 para el lago de datos (landing, bronze, warehouse...),
#   - un rol IAM para el pipeline con permisos mínimos sobre ese bucket.

data "aws_caller_identity" "actual" {}

locals {
  prefijo = "${var.proyecto}-${var.entorno}"
  # Los nombres de bucket son únicos en TODO AWS: el id de la cuenta lo garantiza
  bucket_lago = "${local.prefijo}-lake-${data.aws_caller_identity.actual.account_id}"
  # Carpetas (prefijos) del lago en las que escribe el pipeline
  zonas = ["landing", "origen", "bronze", "warehouse"]
}

# --- Lago de datos ---------------------------------------------------------------

resource "aws_s3_bucket" "lago" {
  bucket = local.bucket_lago
  # destroy vacía el bucket (todas las versiones) antes de borrarlo. Cómodo en local y
  # dev; en prod un destroy por error NO debe poder llevarse los datos por delante.
  force_destroy = var.entorno != "prod"
}

# Versionado: sobrescribir o borrar un objeto conserva la versión anterior.
# Es la red de seguridad ante un error en una ingesta.
resource "aws_s3_bucket_versioning" "lago" {
  bucket = aws_s3_bucket.lago.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Cifrado en reposo de todo lo que se guarde (claves gestionadas por S3)
resource "aws_s3_bucket_server_side_encryption_configuration" "lago" {
  bucket = aws_s3_bucket.lago.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Nada del lago es público, aunque alguien se equivoque al dar permisos a un objeto
resource "aws_s3_bucket_public_access_block" "lago" {
  bucket                  = aws_s3_bucket.lago.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Ciclo de vida: la landing guarda ficheros en bruto ya ingeridos; pasado un tiempo
# se borran solos. Las versiones antiguas (sobrescritas) caducan a los 30 días.
# Tarda ~1 min en crearse: el proveedor espera a leer la misma regla 10 veces
# seguidas, porque en AWS los cambios de ciclo de vida tardan en propagarse.
resource "aws_s3_bucket_lifecycle_configuration" "lago" {
  bucket = aws_s3_bucket.lago.id

  rule {
    id     = "caducar-landing"
    status = "Enabled"
    filter {
      prefix = "landing/"
    }
    expiration {
      days = var.dias_retencion_landing
    }
  }

  rule {
    id     = "caducar-versiones-antiguas"
    status = "Enabled"
    filter {} # todo el bucket
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  depends_on = [aws_s3_bucket_versioning.lago]
}

# --- Identidad del pipeline ------------------------------------------------------

# Quién puede "ponerse" el rol. Aquí, la propia cuenta; en un despliegue real
# serían el servicio que ejecuta el pipeline (p. ej. ECS) o GitHub Actions por OIDC.
data "aws_iam_policy_document" "asumir_rol" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.actual.account_id}:root"]
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${local.prefijo}-pipeline"
  assume_role_policy = data.aws_iam_policy_document.asumir_rol.json
}

# Mínimo privilegio: listar el bucket y leer/escribir/borrar SOLO en las zonas del
# lago. Nada de borrar el bucket, cambiar su configuración ni tocar otros buckets.
data "aws_iam_policy_document" "lago" {
  statement {
    sid       = "ListarElLago"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lago.arn]
  }
  statement {
    sid       = "LeerYEscribirZonas"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [for zona in local.zonas : "${aws_s3_bucket.lago.arn}/${zona}/*"]
  }
}

resource "aws_iam_policy" "lago" {
  name   = "${local.prefijo}-acceso-lago"
  policy = data.aws_iam_policy_document.lago.json
}

resource "aws_iam_role_policy_attachment" "pipeline_lago" {
  role       = aws_iam_role.pipeline.name
  policy_arn = aws_iam_policy.lago.arn
}
