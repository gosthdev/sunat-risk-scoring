variable "role_name" {
  description = "Nombre del rol que asume GitHub Actions."
  type        = string
}

variable "github_repo" {
  description = "Repositorio autorizado, formato organización/repositorio."
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "ARN del bucket de soporte donde el CI sube el código a jobs/."
  type        = string
}
