module "s3_tfstate" {
  source        = "../modules/bucket/s3_data_bucket"
  bucket_name   = var.state_bucket_name
  force_destroy = false

  versioning_enabled                 = true
  protect_versions                   = true
  noncurrent_version_expiration_days = 90
}
