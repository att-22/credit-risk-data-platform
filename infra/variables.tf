# Entradas de la infraestructura. Sus valores por entorno están en entornos/*.tfvars.

variable "entorno" {
  description = "Nombre del entorno: forma parte del nombre de cada recurso."
  type        = string

  validation {
    condition     = contains(["local", "dev", "prod"], var.entorno)
    error_message = "El entorno debe ser local, dev o prod."
  }
}

variable "region" {
  description = "Región de AWS."
  type        = string
  default     = "eu-west-1" # Irlanda: la región europea de referencia
}

variable "proyecto" {
  description = "Prefijo de todos los recursos."
  type        = string
  default     = "credit-risk"
}

variable "endpoint_aws" {
  description = <<-EOT
    URL de un emulador de AWS (p. ej. Moto). null = AWS de verdad. Es lo único que
    cambia entre practicar en local y desplegar en la nube.
  EOT
  type        = string
  default     = null
}

variable "dias_retencion_landing" {
  description = "Días que se conservan las entregas en bruto de la landing zone."
  type        = number
  default     = 30

  validation {
    condition     = var.dias_retencion_landing >= 1
    error_message = "La retención debe ser de al menos un día."
  }
}
