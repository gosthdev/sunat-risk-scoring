############ Costo ############
# Presupuesto de la cuenta: avisa al 50/80/100 % del gasto real y cuando el
# pronóstico del mes supera el límite. Es de toda la cuenta (no filtra por tag)
# para no depender de que el tag Project esté activado en Billing.
module "cost_budget" {
  source            = "../modules/cost_budget"
  budget_name       = "sunat-ssco-monthly"
  monthly_limit_usd = var.monthly_budget_usd
  alert_emails      = var.budget_alert_emails
}
