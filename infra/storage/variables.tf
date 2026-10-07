variable "aws_region" {
  description = "Región de AWS donde se despliegan los buckets S3."
  type        = string
}

variable "project_tag" {
  description = "Valor del tag Project (sirve para filtrar costos en Billing)."
  type        = string
}

variable "raw_bucket_name" {
  description = "Bucket de la zona raw (nombre globalmente único)."
  type        = string
}

variable "bronze_bucket_name" {
  description = "Bucket de la zona bronze (incluye el prefijo quarantine/)."
  type        = string
}

variable "silver_bucket_name" {
  description = "Bucket de la zona silver."
  type        = string
}

variable "gold_bucket_name" {
  description = "Bucket de la zona gold."
  type        = string
}

variable "artifacts_bucket_name" {
  description = "Bucket de soporte: código de los jobs (jobs/), logs de EMR (emr-logs/) y resultados de Athena (athena-results/)."
  type        = string
}

variable "force_destroy_buckets" {
  description = "true permite que terraform destroy borre buckets con datos. Mantener false salvo que desees eliminar datos."
  type        = bool
  default     = false
}
