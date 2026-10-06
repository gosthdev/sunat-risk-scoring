variable "aws_region" {
  description = "Región de AWS donde se despliega todo."
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
  description = "true permite que terraform destroy borre buckets con datos. Mantener false salvo al desmontar el proyecto."
  type        = bool
}

variable "emr_release_label" {
  description = "Versión de EMR Serverless (ej. emr-7.13.0)."
  type        = string
}

variable "emr_max_cpu" {
  description = "Tope de vCPU simultáneas de la aplicación EMR Serverless (límite de costo)."
  type        = string
}

variable "emr_max_memory" {
  description = "Tope de memoria simultánea de la aplicación EMR Serverless."
  type        = string
}

variable "emr_max_disk" {
  description = "Tope de disco simultáneo de la aplicación EMR Serverless."
  type        = string
}

variable "emr_idle_timeout_minutes" {
  description = "Minutos sin jobs antes de que la aplicación EMR Serverless se apague sola."
  type        = number
}

variable "athena_bytes_scanned_cutoff" {
  description = "Máximo de bytes que puede escanear una consulta de Athena (límite de costo)."
  type        = number
}

variable "github_repo" {
  description = "Repositorio de GitHub autorizado a asumir el rol de CI, formato organización/repositorio."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$", var.github_repo))
    error_message = "github_repo debe tener formato organizacion/repositorio. Reemplaza el placeholder en terraform.tfvars."
  }
}
