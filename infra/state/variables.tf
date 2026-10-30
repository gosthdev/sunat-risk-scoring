variable "aws_region" {
  description = "Región de AWS donde se crea el bucket de estado."
  type        = string
}

variable "project_tag" {
  description = "Valor del tag Project (sirve para filtrar costos en Billing)."
  type        = string
}

variable "state_bucket_name" {
  description = "Bucket del estado de Terraform (nombre globalmente único). Debe coincidir con el bucket de los bloques backend de storage y services."
  type        = string
}
