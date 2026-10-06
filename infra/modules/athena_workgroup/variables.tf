variable "workgroup_name" {
  description = "Nombre del workgroup de Athena."
  type        = string
}

variable "results_bucket_name" {
  description = "Bucket donde Athena guarda resultados (prefijo athena-results/)."
  type        = string
}

variable "bytes_scanned_cutoff_per_query" {
  description = "Máximo de bytes escaneados por consulta (límite de costo)."
  type        = number
}
