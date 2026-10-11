variable "role_name" {
  type        = string
  description = "Nombre del rol IAM (y del perfil de instancia) de la EC2 efímera de entrenamiento."
  default     = "sunat-ssco-train-ec2-role"
}

variable "gold_bucket_arn" {
  type        = string
  description = "ARN del bucket S3 de la zona gold."
}

variable "artifacts_bucket_arn" {
  type        = string
  description = "ARN del bucket S3 de artefactos (de donde la instancia descarga el código)."
}

variable "tags" {
  type        = map(string)
  description = "Etiquetas aplicadas al rol IAM."
  default     = {}
}
