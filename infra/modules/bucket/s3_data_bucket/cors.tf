resource "aws_s3_bucket_cors_configuration" "this" {
  count  = length(var.cors_allowed_origins) > 0 ? 1 : 0
  bucket = aws_s3_bucket.this.id

  cors_rule {
    allowed_methods = ["POST"]
    allowed_origins = var.cors_allowed_origins
    allowed_headers = ["content-type"]
    max_age_seconds = 3000
  }
}
