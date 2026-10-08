variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "Región de AWS donde se despliegan los recursos."
}

variable "aws_profile" {
  type        = string
  default     = "bigdata"
  description = "Perfil de AWS CLI configurado localmente."
}

variable "project_tag" {
  type        = string
  default     = "sunat-risk-scoring"
  description = "Tag de proyecto para cost tracking."
}

variable "raw_bucket_name" {
  type        = string
  default     = "sunat-risk-scoring-raw"
  description = "Nombre del bucket S3 zona Raw donde se subirán los datos."
}

variable "instance_type" {
  type        = string
  default     = "c6i.large"
  description = "Tipo de instancia EC2 para procesamiento paralelo y descompresión rápida (ej. c6i.large o t3.medium)."
}

variable "ebs_volume_size" {
  type        = number
  default     = 60
  description = "Tamaño del disco EBS gp3 en GB (se borra al terminar la instancia)."
}
