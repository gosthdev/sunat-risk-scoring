output "raw_bucket" {
  value = module.s3_raw.bucket_name
}

output "bronze_bucket" {
  value = module.s3_bronze.bucket_name
}

output "silver_bucket" {
  value = module.s3_silver.bucket_name
}

output "gold_bucket" {
  value = module.s3_gold.bucket_name
}

output "artifacts_bucket" {
  value = module.s3_artifacts.bucket_name
}

output "emr_application_id" {
  value = module.emr_serverless_app.application_id
}

output "emr_execution_role_arn" {
  value = module.iam_emr_execution_role.role_arn
}

output "glue_database" {
  value = module.glue_database.database_name
}

output "glue_crawler_silver" {
  value = module.glue_crawler_silver.crawler_name
}

output "glue_crawler_gold" {
  value = module.glue_crawler_gold.crawler_name
}

output "athena_workgroup" {
  value = module.athena_workgroup.workgroup_name
}

output "github_actions_role_arn" {
  value = module.iam_github_actions_role.role_arn
}
