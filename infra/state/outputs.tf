output "state_bucket_name" {
  value       = module.s3_tfstate.bucket_name
  description = "Nombre del bucket de estado de Terraform."
}

output "state_bucket_arn" {
  value       = module.s3_tfstate.bucket_arn
  description = "ARN del bucket de estado de Terraform."
}
