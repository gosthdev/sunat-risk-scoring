resource "aws_s3_bucket_lifecycle_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-emr-logs"
    status = "Enabled"

    filter {
      prefix = "emr-logs/"
    }

    expiration {
      days = var.expiration_days
    }
  }

  # pipeline.yml sube el código de cada corrida a jobs/releases/<sha>/.
  rule {
    id     = "expire-job-releases"
    status = "Enabled"

    filter {
      prefix = "jobs/releases/"
    }

    expiration {
      days = var.expiration_days
    }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = var.expiration_days
    }
  }
}
