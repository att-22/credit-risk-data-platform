variable "repositorio" {
  description = "Repositorio de GitHub (propietario/nombre) que puede desplegar."
  type        = string
  default     = "att-22/credit-risk-data-platform"

  validation {
    condition     = can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.repositorio))
    error_message = "Formato propietario/nombre, sin comodines."
  }
}

variable "entorno_github" {
  description = <<-EOT
    Environment de GitHub desde el que se permite desplegar. Solo los jobs que lo
    declaran pueden asumir el rol, y el environment puede exigir una aprobación manual.
  EOT
  type        = string
  default     = "aws-dev"
}

variable "region" {
  description = "Región de AWS."
  type        = string
  default     = "eu-west-1"
}

variable "proyecto" {
  description = "Prefijo de todos los recursos."
  type        = string
  default     = "credit-risk"
}

variable "endpoint_aws" {
  description = "URL de un emulador de AWS (p. ej. Moto). null = AWS de verdad."
  type        = string
  default     = null
}
