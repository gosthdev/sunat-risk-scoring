output "role_arn" {
  value       = aws_iam_role.this.arn
  description = "ARN del rol IAM de ejecución para SageMaker."
}

output "role_name" {
  value       = aws_iam_role.this.name
  description = "Nombre del rol IAM de ejecución para SageMaker."
}
