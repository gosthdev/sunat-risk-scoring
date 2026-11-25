variable "raw_bucket_arn" {
  description = "ARN del bucket raw: el único bucket al que la EC2 de ingesta puede escribir."
  type        = string
}

variable "bronze_bucket_arn" {
  description = "ARN del bucket bronze: la EC2 solo puede escribir/borrar bajo _pendientes/."
  type        = string
}
