variable "crawler_name" {
  description = "Nombre del crawler."
  type        = string
}

variable "database_name" {
  description = "Base de datos del catálogo donde se crean las tablas."
  type        = string
}

variable "role_arn" {
  description = "ARN del rol que usa el crawler."
  type        = string
}

variable "s3_target_path" {
  description = "Ruta S3 a catalogar (s3://bucket/). Cada carpeta de primer nivel se vuelve una tabla."
  type        = string
}

variable "table_prefix" {
  description = "Prefijo de las tablas creadas (evita choques entre silver y gold)."
  type        = string
}

variable "extra_exclusions" {
  description = "Patrones S3 adicionales a excluir (relativos a s3_target_path)."
  type        = list(string)
  default     = []
}

variable "table_level" {
  description = "TableLevelConfiguration: nivel ABSOLUTO desde la raíz del bucket donde se crea la tabla (carpeta de primer nivel = 1)."
  type        = number
  default     = 2
}
