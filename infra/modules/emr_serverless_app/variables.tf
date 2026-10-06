variable "application_name" {
  description = "Nombre de la aplicación EMR Serverless."
  type        = string
}

variable "release_label" {
  description = "Versión de EMR Serverless (ej. emr-7.13.0)."
  type        = string
}

variable "max_cpu" {
  description = "Tope de vCPU simultáneas."
  type        = string
}

variable "max_memory" {
  description = "Tope de memoria simultánea."
  type        = string
}

variable "max_disk" {
  description = "Tope de disco simultáneo."
  type        = string
}

variable "idle_timeout_minutes" {
  description = "Minutos de inactividad antes del apagado automático."
  type        = number
}
