output "role_arn" {
  value       = aws_iam_role.this.arn
  description = "ARN del rol IAM de la EC2 de entrenamiento."
}

output "role_name" {
  value       = aws_iam_role.this.name
  description = "Nombre del rol IAM de la EC2 de entrenamiento."
}

output "instance_profile_name" {
  value       = aws_iam_instance_profile.this.name
  description = "Nombre del perfil de instancia que se asocia a la EC2 de entrenamiento."
}
