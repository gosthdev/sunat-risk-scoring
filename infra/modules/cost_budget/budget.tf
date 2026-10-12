resource "aws_budgets_budget" "monthly" {
  name         = var.budget_name
  budget_type  = "COST"
  time_unit    = "MONTHLY"
  limit_amount = tostring(var.monthly_limit_usd)
  limit_unit   = "USD"

  # Avisos sobre el gasto real al cruzar cada umbral.
  dynamic "notification" {
    for_each = var.actual_thresholds_percent

    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = var.alert_emails
    }
  }

  # Aviso anticipado si el pronóstico del mes supera el límite.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = var.alert_emails
  }
}
