variable "bucket_name" {
  description = "Nombre globalmente único del bucket."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "Nombre de bucket inválido (solo minúsculas, números y guiones). Si ves <ACCOUNT_ID>, reemplázalo en terraform.tfvars."
  }
}

variable "force_destroy" {
  description = "Permite borrar el bucket aunque tenga objetos."
  type        = bool
}
