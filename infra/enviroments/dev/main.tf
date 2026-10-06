############ S3 ############
module "s3_raw" {
  source        = "../../modules/bucket/s3_data_bucket"
  bucket_name   = var.raw_bucket_name
  force_destroy = var.force_destroy_buckets
}

module "s3_bronze" {
  source        = "../../modules/bucket/s3_data_bucket"
  bucket_name   = var.bronze_bucket_name
  force_destroy = var.force_destroy_buckets
}

module "s3_silver" {
  source        = "../../modules/bucket/s3_data_bucket"
  bucket_name   = var.silver_bucket_name
  force_destroy = var.force_destroy_buckets
}

module "s3_gold" {
  source        = "../../modules/bucket/s3_data_bucket"
  bucket_name   = var.gold_bucket_name
  force_destroy = var.force_destroy_buckets
}

module "s3_artifacts" {
  source          = "../../modules/bucket/s3_artifacts_bucket"
  bucket_name     = var.artifacts_bucket_name
  force_destroy   = var.force_destroy_buckets
  expiration_days = 30
}

############ IAM ############
module "iam_emr_execution_role" {
  source               = "../../modules/security/iam_emr_execution_role"
  role_name            = "sunat-ssco-emr-serverless-role"
  raw_bucket_arn       = module.s3_raw.bucket_arn
  bronze_bucket_arn    = module.s3_bronze.bucket_arn
  silver_bucket_arn    = module.s3_silver.bucket_arn
  gold_bucket_arn      = module.s3_gold.bucket_arn
  artifacts_bucket_arn = module.s3_artifacts.bucket_arn
}

module "iam_glue_crawler_role" {
  source            = "../../modules/security/iam_glue_crawler_role"
  role_name         = "sunat-ssco-glue-crawler-role"
  silver_bucket_arn = module.s3_silver.bucket_arn
  gold_bucket_arn   = module.s3_gold.bucket_arn
}

module "iam_github_actions_role" {
  source               = "../../modules/security/iam_github_actions_role"
  role_name            = "sunat-ssco-github-actions-role"
  github_repo          = var.github_repo
  artifacts_bucket_arn = module.s3_artifacts.bucket_arn
}

############ EMR Serverless ############
module "emr_serverless_app" {
  source               = "../../modules/emr_serverless_app"
  application_name     = "sunat-ssco-spark"
  release_label        = var.emr_release_label
  max_cpu              = var.emr_max_cpu
  max_memory           = var.emr_max_memory
  max_disk             = var.emr_max_disk
  idle_timeout_minutes = var.emr_idle_timeout_minutes
}

############ Glue ############
module "glue_database" {
  source        = "../../modules/glue/glue_database"
  database_name = "sunat_ssco"
  description   = "Tablas Parquet de las zonas silver y gold del proyecto SSCO."
}

module "glue_crawler_silver" {
  source         = "../../modules/glue/glue_crawler"
  crawler_name   = "sunat-ssco-silver-crawler"
  database_name  = module.glue_database.database_name
  role_arn       = module.iam_glue_crawler_role.role_arn
  s3_target_path = "s3://${module.s3_silver.bucket_name}/"
  table_prefix   = "silver_"
}

module "glue_crawler_gold" {
  source         = "../../modules/glue/glue_crawler"
  crawler_name   = "sunat-ssco-gold-crawler"
  database_name  = module.glue_database.database_name
  role_arn       = module.iam_glue_crawler_role.role_arn
  s3_target_path = "s3://${module.s3_gold.bucket_name}/"
  table_prefix   = "gold_"
}

############ Athena ############
module "athena_workgroup" {
  source                         = "../../modules/athena_workgroup"
  workgroup_name                 = "sunat-ssco"
  results_bucket_name            = module.s3_artifacts.bucket_name
  bytes_scanned_cutoff_per_query = var.athena_bytes_scanned_cutoff
}
