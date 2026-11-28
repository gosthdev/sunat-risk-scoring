variable "user_name" {
  description = "Nombre del usuario IAM del portal web de subida."
  type        = string
}

variable "raw_bucket_arn" {
  description = "ARN del bucket raw; el usuario solo escribe bajo _inbox/."
  type        = string
}
