variable "role_name" {
  type        = string
  description = "Nombre del rol IAM de ejecución para SageMaker Training Jobs."
  default     = "sunat-ssco-sagemaker-execution-role"
}

variable "gold_bucket_arn" {
  type        = string
  description = "ARN del bucket S3 de la zona gold."
}

variable "tags" {
  type        = map(string)
  description = "Etiquetas aplicadas al rol IAM."
  default     = {}
}
