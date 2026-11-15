variable "role_name" {
  description = "Nombre del rol que asume QuickSight para leer Athena, Glue y S3."
  type        = string
}

variable "aws_region" {
  description = "Región de AWS (ARNs de Athena y Glue)."
  type        = string
}

variable "workgroup_name" {
  description = "Workgroup de Athena que QuickSight puede usar."
  type        = string
}

variable "glue_database_name" {
  description = "Base del Glue Data Catalog que QuickSight puede leer."
  type        = string
}

variable "gold_bucket_arn" {
  description = "ARN del bucket gold (solo lectura de los prefijos publicados para dashboards)."
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "ARN del bucket artifacts (resultados de Athena en athena-results/)."
  type        = string
}
