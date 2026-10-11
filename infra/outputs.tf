# Salidas: valores que otros (el pipeline, la CI) necesitan saber tras el apply.

output "bucket_lago" {
  description = "Nombre del bucket S3 del lago."
  value       = aws_s3_bucket.lago.bucket
}

output "uri_lago" {
  description = "URI del lago para el pipeline (LAKE_DIR en la Fase 5.2)."
  value       = "s3://${aws_s3_bucket.lago.bucket}"
}

output "rol_pipeline" {
  description = "ARN del rol IAM con el que se ejecuta el pipeline."
  value       = aws_iam_role.pipeline.arn
}
