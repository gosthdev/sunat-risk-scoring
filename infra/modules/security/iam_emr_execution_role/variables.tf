variable "role_name" {
  description = "Nombre del rol de ejecución de los jobs de EMR Serverless."
  type        = string
}

variable "raw_bucket_arn" {
  description = "ARN del bucket raw (solo lectura)."
  type        = string
}

variable "bronze_bucket_arn" {
  description = "ARN del bucket bronze (lectura y escritura)."
  type        = string
}

variable "silver_bucket_arn" {
  description = "ARN del bucket silver (lectura y escritura)."
  type        = string
}

variable "gold_bucket_arn" {
  description = "ARN del bucket gold (lectura y escritura)."
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "ARN del bucket de soporte (lee jobs/, escribe emr-logs/)."
  type        = string
}
