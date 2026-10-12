variable "budget_name" {
  description = "Nombre del presupuesto mensual."
  type        = string
}

variable "monthly_limit_usd" {
  description = "Límite mensual de gasto en USD."
  type        = number

  validation {
    condition     = var.monthly_limit_usd > 0
    error_message = "monthly_limit_usd debe ser mayor que 0."
  }
}

variable "alert_emails" {
  description = "Correos que reciben las alertas del presupuesto."
  type        = list(string)

  validation {
    condition     = length(var.alert_emails) > 0
    error_message = "Indica al menos un correo para las alertas de costo."
  }
}

variable "actual_thresholds_percent" {
  description = "Umbrales (% del límite) sobre el gasto real que disparan una alerta."
  type        = list(number)
  default     = [50, 80, 100]
}
