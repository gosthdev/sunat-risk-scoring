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

variable "versioning_enabled" {
  description = "Activa el versionado del bucket (permite recuperar objetos sobrescritos o borrados)."
  type        = bool
  default     = false
}

variable "noncurrent_version_expiration_days" {
  description = "Días que se conservan las versiones reemplazadas antes de eliminarlas (solo con versionado)."
  type        = number
  default     = 30

  validation {
    condition     = var.noncurrent_version_expiration_days >= 1
    error_message = "noncurrent_version_expiration_days debe ser al menos 1."
  }
}

variable "protect_versions" {
  description = "true deniega s3:DeleteObjectVersion a todos: los datos no se pueden borrar de forma permanente. Requiere versioning_enabled."
  type        = bool
  default     = false

  validation {
    condition     = !var.protect_versions || var.versioning_enabled
    error_message = "protect_versions solo tiene sentido con versioning_enabled = true."
  }
}

variable "inbox_lifecycle_enabled" {
  description = "true agrega una regla que expira _inbox/ y aborta multipart incompletos al día."
  type        = bool
  default     = false
}

variable "inbox_expiration_days" {
  description = "Días tras los que expiran los objetos de _inbox/ (solo con inbox_lifecycle_enabled)."
  type        = number
  default     = 7
}

variable "cors_allowed_origins" {
  description = "Orígenes exactos (sin comodines) autorizados a hacer POST desde el navegador. Vacío = sin CORS."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for o in var.cors_allowed_origins : !strcontains(o, "*")])
    error_message = "cors_allowed_origins no admite comodines."
  }
}
