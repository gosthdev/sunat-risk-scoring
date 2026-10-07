output "raw_bucket_name" {
  value       = module.s3_raw.bucket_name
  description = "Nombre del bucket S3 zona Raw."
}

output "raw_bucket_arn" {
  value       = module.s3_raw.bucket_arn
  description = "ARN del bucket S3 zona Raw."
}

output "bronze_bucket_name" {
  value       = module.s3_bronze.bucket_name
  description = "Nombre del bucket S3 zona Bronze."
}

output "bronze_bucket_arn" {
  value       = module.s3_bronze.bucket_arn
  description = "ARN del bucket S3 zona Bronze."
}

output "silver_bucket_name" {
  value       = module.s3_silver.bucket_name
  description = "Nombre del bucket S3 zona Silver."
}

output "silver_bucket_arn" {
  value       = module.s3_silver.bucket_arn
  description = "ARN del bucket S3 zona Silver."
}

output "gold_bucket_name" {
  value       = module.s3_gold.bucket_name
  description = "Nombre del bucket S3 zona Gold."
}

output "gold_bucket_arn" {
  value       = module.s3_gold.bucket_arn
  description = "ARN del bucket S3 zona Gold."
}

output "artifacts_bucket_name" {
  value       = module.s3_artifacts.bucket_name
  description = "Nombre del bucket S3 zona Artifacts."
}

output "artifacts_bucket_arn" {
  value       = module.s3_artifacts.bucket_arn
  description = "ARN del bucket S3 zona Artifacts."
}
