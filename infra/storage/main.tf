############ S3 Storage Stack ############

# Raw es la fuente de verdad del reproceso: versionado y sin borrado permanente.
module "s3_raw" {
  source        = "../modules/bucket/s3_data_bucket"
  bucket_name   = var.raw_bucket_name
  force_destroy = var.force_destroy_buckets

  versioning_enabled                 = true
  protect_versions                   = true
  noncurrent_version_expiration_days = 90
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

# Gold versionado: si una publicación sale mal se puede volver a la versión anterior.
module "s3_gold" {
  source        = "../modules/bucket/s3_data_bucket"
  bucket_name   = var.gold_bucket_name
  force_destroy = var.force_destroy_buckets

  versioning_enabled                 = true
  noncurrent_version_expiration_days = 30
}

module "s3_artifacts" {
  source          = "../modules/bucket/s3_artifacts_bucket"
  bucket_name     = var.artifacts_bucket_name
  force_destroy   = var.force_destroy_buckets
  expiration_days = 30
}
