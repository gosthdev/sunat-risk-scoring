variable "role_name" {
  description = "Nombre del rol del crawler de Glue."
  type        = string
}

variable "silver_bucket_arn" {
  description = "ARN del bucket silver (solo lectura)."
  type        = string
}

variable "gold_bucket_arn" {
  description = "ARN del bucket gold (solo lectura)."
  type        = string
}
