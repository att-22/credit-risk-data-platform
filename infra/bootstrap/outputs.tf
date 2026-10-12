# Los dos valores que hay que copiar a las variables del repositorio en GitHub
# (Settings > Secrets and variables > Actions > Variables) para activar el despliegue.

output "bucket_estado" {
  description = "Variable TF_STATE_BUCKET: bucket del estado de Terraform."
  value       = aws_s3_bucket.estado.bucket
}

output "rol_despliegue" {
  description = "Variable AWS_DEPLOY_ROLE_ARN: rol que asume GitHub Actions."
  value       = aws_iam_role.despliegue.arn
}
