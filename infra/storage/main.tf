############ S3 Storage Stack ############

module "s3_raw" {
  source        = "../modules/bucket/s3_data_bucket"
  bucket_name   = var.raw_bucket_name
  force_destroy = var.force_destroy_buckets
}

module "s3_bronze" {
  source        = "../modules/bucket/s3_data_bucket"
  bucket_name   = var.bronze_bucket_name
  force_destroy = var.force_destroy_buckets
}

module "s3_silver" {
  source        = "../modules/bucket/s3_data_bucket"
  bucket_name   = var.silver_bucket_name
  force_destroy = var.force_destroy_buckets
}

module "s3_gold" {
  source        = "../modules/bucket/s3_data_bucket"
  bucket_name   = var.gold_bucket_name
  force_destroy = var.force_destroy_buckets
}

module "s3_artifacts" {
  source          = "../modules/bucket/s3_artifacts_bucket"
  bucket_name     = var.artifacts_bucket_name
  force_destroy   = var.force_destroy_buckets
  expiration_days = 30
}
