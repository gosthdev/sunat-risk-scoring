############ Data Sources (S3 Buckets) ############
data "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket_name
}

data "aws_s3_bucket" "bronze" {
  bucket = var.bronze_bucket_name
}

data "aws_s3_bucket" "silver" {
  bucket = var.silver_bucket_name
}

data "aws_s3_bucket" "gold" {
  bucket = var.gold_bucket_name
}

data "aws_s3_bucket" "artifacts" {
  bucket = var.artifacts_bucket_name
}

############ IAM ############
module "iam_emr_execution_role" {
  source               = "../modules/security/iam_emr_execution_role"
  role_name            = "sunat-ssco-emr-serverless-role"
  raw_bucket_arn       = data.aws_s3_bucket.raw.arn
  bronze_bucket_arn    = data.aws_s3_bucket.bronze.arn
  silver_bucket_arn    = data.aws_s3_bucket.silver.arn
  gold_bucket_arn      = data.aws_s3_bucket.gold.arn
  artifacts_bucket_arn = data.aws_s3_bucket.artifacts.arn
}

module "iam_sagemaker_execution_role" {
  source          = "../modules/security/iam_sagemaker_execution_role"
  role_name       = "sunat-ssco-sagemaker-execution-role"
  gold_bucket_arn = data.aws_s3_bucket.gold.arn
}

module "iam_glue_crawler_role" {
  source            = "../modules/security/iam_glue_crawler_role"
  role_name         = "sunat-ssco-glue-crawler-role"
  silver_bucket_arn = data.aws_s3_bucket.silver.arn
  gold_bucket_arn   = data.aws_s3_bucket.gold.arn
}

module "iam_github_actions_role" {
  source                 = "../modules/security/iam_github_actions_role"
  role_name              = "sunat-ssco-github-actions-role"
  github_repo            = var.github_repo
  artifacts_bucket_arn   = data.aws_s3_bucket.artifacts.arn
  raw_bucket_arn         = data.aws_s3_bucket.raw.arn
  bronze_bucket_arn      = data.aws_s3_bucket.bronze.arn
  silver_bucket_arn      = data.aws_s3_bucket.silver.arn
  gold_bucket_arn        = data.aws_s3_bucket.gold.arn
  emr_application_arn    = module.emr_serverless_app.application_arn
  emr_execution_role_arn = module.iam_emr_execution_role.role_arn
  glue_crawler_role_arn  = module.iam_glue_crawler_role.role_arn
}

############ EMR Serverless ############
module "emr_serverless_app" {
  source               = "../modules/emr_serverless_app"
  application_name     = "sunat-ssco-spark"
  release_label        = var.emr_release_label
  max_cpu              = var.emr_max_cpu
  max_memory           = var.emr_max_memory
  max_disk             = var.emr_max_disk
  idle_timeout_minutes = var.emr_idle_timeout_minutes
}

############ Glue ############
module "glue_database" {
  source        = "../modules/glue/glue_database"
  database_name = "sunat_ssco"
  description   = "Tablas Parquet de las zonas silver y gold del proyecto SSCO."
}

module "glue_crawler_silver" {
  source         = "../modules/glue/glue_crawler"
  crawler_name   = "sunat-ssco-silver-crawler"
  database_name  = module.glue_database.database_name
  role_arn       = module.iam_glue_crawler_role.role_arn
  s3_target_path = "s3://${data.aws_s3_bucket.silver.bucket}/"
  table_prefix   = "silver_"
}

module "glue_crawler_gold" {
  source         = "../modules/glue/glue_crawler"
  crawler_name   = "sunat-ssco-gold-crawler"
  database_name  = module.glue_database.database_name
  role_arn       = module.iam_glue_crawler_role.role_arn
  s3_target_path = "s3://${data.aws_s3_bucket.gold.bucket}/"
  table_prefix   = "gold_"
}

############ Athena ############
module "athena_workgroup" {
  source                         = "../modules/athena_workgroup"
  workgroup_name                 = "sunat-ssco"
  results_bucket_name            = data.aws_s3_bucket.artifacts.bucket
  bytes_scanned_cutoff_per_query = var.athena_bytes_scanned_cutoff
}
