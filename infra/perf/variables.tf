variable "aws_region" {
  description = "Región de AWS donde se despliegan los recursos de pruebas de rendimiento."
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Perfil de credenciales de AWS local."
  type        = string
  default     = "bigdata"
}

variable "project_tag" {
  description = "Valor del tag Project."
  type        = string
  default     = "sunat-risk-scoring"
}

variable "environment" {
  description = "Ambiente de despliegue."
  type        = string
  default     = "perf"
}

variable "bucket_perf_36p" {
  description = "Nombre del bucket S3 para experimento 3.6 Layout P (particionado)."
  type        = string
  default     = "sunat-risk-scoring-perf-36p"
}

variable "bucket_perf_36n" {
  description = "Nombre del bucket S3 para experimento 3.6 Layout N (sin particionar)."
  type        = string
  default     = "sunat-risk-scoring-perf-36n"
}

variable "bucket_perf_e2" {
  description = "Nombre del bucket S3 para experimento E2 (persistencia CSV)."
  type        = string
  default     = "sunat-risk-scoring-perf-e2"
}

variable "bucket_perf_x1" {
  description = "Nombre del bucket S3 para pruebas de escala 1x."
  type        = string
  default     = "sunat-risk-scoring-perf-x1"
}

variable "bucket_perf_x10" {
  description = "Nombre del bucket S3 para pruebas de escala 10x."
  type        = string
  default     = "sunat-risk-scoring-perf-x10"
}

variable "bucket_perf_x100" {
  description = "Nombre del bucket S3 para pruebas de escala 100x."
  type        = string
  default     = "sunat-risk-scoring-perf-x100"
}

variable "raw_bucket_name" {
  description = "Nombre del bucket real raw."
  type        = string
  default     = "sunat-risk-scoring-raw"
}

variable "bronze_bucket_name" {
  description = "Nombre del bucket real bronze."
  type        = string
  default     = "sunat-risk-scoring-bronze"
}

variable "silver_bucket_name" {
  description = "Nombre del bucket real silver."
  type        = string
  default     = "sunat-risk-scoring-silver"
}

variable "gold_bucket_name" {
  description = "Nombre del bucket real gold."
  type        = string
  default     = "sunat-risk-scoring-gold"
}

variable "artifacts_bucket_name" {
  description = "Nombre del bucket real de artefactos."
  type        = string
  default     = "sunat-risk-scoring-artifacts"
}

variable "emr_release_label" {
  description = "Release label de EMR Serverless para pruebas."
  type        = string
  default     = "emr-7.13.0"
}

variable "emr_max_cpu" {
  description = "Límite máximo de CPU para la aplicación EMR Serverless de pruebas."
  type        = string
  default     = "32 vCPU"
}

variable "emr_max_memory" {
  description = "Límite máximo de memoria para la aplicación EMR Serverless de pruebas."
  type        = string
  default     = "128 GB"
}

variable "emr_max_disk" {
  description = "Límite máximo de disco para la aplicación EMR Serverless de pruebas."
  type        = string
  default     = "500 GB"
}

variable "emr_idle_timeout_minutes" {
  description = "Minutos de inactividad antes de apagar workers en EMR Serverless."
  type        = number
  default     = 15
}

variable "budget_limit_amount" {
  description = "Monto límite en USD para el presupuesto de pruebas."
  type        = string
  default     = "100.0"
}

variable "budget_notification_emails" {
  description = "Lista opcional de correos para alertas del presupuesto."
  type        = list(string)
  default     = []
}

variable "force_destroy_buckets" {
  description = "Permite destruir buckets de prueba con contenido al desmontar infra."
  type        = bool
  default     = true
}
