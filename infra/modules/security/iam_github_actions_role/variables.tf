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

variable "raw_bucket_arn" {
  description = "ARN del bucket de la zona raw."
  type        = string
}

variable "silver_bucket_arn" {
  description = "ARN del bucket de la zona silver."
  type        = string
}

variable "emr_application_arn" {
  description = "ARN de la aplicación EMR Serverless."
  type        = string
}

variable "emr_execution_role_arn" {
  description = "ARN del rol de ejecución de EMR Serverless para pass_role."
  type        = string
}
