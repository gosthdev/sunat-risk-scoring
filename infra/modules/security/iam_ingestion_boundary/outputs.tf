output "policy_arn" {
  description = "ARN del permissions boundary del rol de ingesta."
  value       = aws_iam_policy.ingestion_boundary.arn
}
