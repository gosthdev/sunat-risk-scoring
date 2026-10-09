############ Data Sources (Real Buckets para Permisos de Solo Lectura) ############

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

############ S3 Buckets de Rendimiento ############

locals {
  standard_perf_buckets = {
    "perf-36p" = var.bucket_perf_36p
    "perf-36n" = var.bucket_perf_36n
    "perf-e2"  = var.bucket_perf_e2
    "perf-x1"  = var.bucket_perf_x1
    "perf-x10" = var.bucket_perf_x10
  }
}

# Buckets estándar con expiración de 14 días
resource "aws_s3_bucket" "standard_perf" {
  for_each      = local.standard_perf_buckets
  bucket        = each.value
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket_public_access_block" "standard_perf" {
  for_each                = aws_s3_bucket.standard_perf
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "standard_perf" {
  for_each = aws_s3_bucket.standard_perf
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "standard_perf" {
  for_each = aws_s3_bucket.standard_perf
  bucket   = each.value.id

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-perf-data-14d"
    status = "Enabled"

    filter {}

    expiration {
      days = 14
    }
  }
}

# Bucket x100 con expiración de 21 días
resource "aws_s3_bucket" "perf_x100" {
  bucket        = var.bucket_perf_x100
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket_public_access_block" "perf_x100" {
  bucket                  = aws_s3_bucket.perf_x100.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "perf_x100" {
  bucket = aws_s3_bucket.perf_x100.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "perf_x100" {
  bucket = aws_s3_bucket.perf_x100.id

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-perf-data-21d"
    status = "Enabled"

    filter {}

    expiration {
      days = 21
    }
  }
}

locals {
  all_perf_bucket_arns = concat(
    [for b in aws_s3_bucket.standard_perf : b.arn],
    [aws_s3_bucket.perf_x100.arn]
  )
}

############ IAM: Rol de Ejecución EMR para Pruebas ############

resource "aws_iam_role" "emr_perf_role" {
  name = "sunat-ssco-perf-emr-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "emr-serverless.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "emr_perf_s3_policy" {
  name = "perf-s3-access"
  role = aws_iam_role.emr_perf_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListRealAndPerfBuckets"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads"
        ]
        Resource = concat(
          [
            data.aws_s3_bucket.raw.arn,
            data.aws_s3_bucket.bronze.arn,
            data.aws_s3_bucket.silver.arn,
            data.aws_s3_bucket.gold.arn,
            data.aws_s3_bucket.artifacts.arn
          ],
          local.all_perf_bucket_arns
        )
      },
      {
        Sid    = "ReadRealDataLayers"
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = [
          "${data.aws_s3_bucket.raw.arn}/*",
          "${data.aws_s3_bucket.bronze.arn}/*",
          "${data.aws_s3_bucket.silver.arn}/*",
          "${data.aws_s3_bucket.gold.arn}/*"
        ]
      },
      {
        Sid    = "ReadJobsAndWriteLogs"
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = [
          "${data.aws_s3_bucket.artifacts.arn}/jobs/*"
        ]
      },
      {
        Sid    = "WriteEmrLogs"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload"
        ]
        Resource = [
          "${data.aws_s3_bucket.artifacts.arn}/emr-logs/*"
        ]
      },
      {
        Sid    = "FullReadWriteOnPerfBuckets"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = [for b_arn in local.all_perf_bucket_arns : "${b_arn}/*"]
      },
      {
        Sid    = "ExplicitDenyWriteOnRealBuckets"
        Effect = "Deny"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${data.aws_s3_bucket.raw.arn}/*",
          "${data.aws_s3_bucket.bronze.arn}/*",
          "${data.aws_s3_bucket.silver.arn}/*",
          "${data.aws_s3_bucket.gold.arn}/*"
        ]
      }
    ]
  })
}

############ EMR Serverless: Aplicación de Rendimiento ############

resource "aws_emrserverless_application" "perf_spark" {
  name          = "sunat-ssco-perf-spark"
  release_label = var.emr_release_label
  type          = "spark"
  architecture  = "X86_64"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.emr_idle_timeout_minutes
  }

  maximum_capacity {
    cpu    = var.emr_max_cpu
    memory = var.emr_max_memory
    disk   = var.emr_max_disk
  }
}

############ Glue: Base de Datos y Crawlers de Rendimiento ############

resource "aws_glue_catalog_database" "perf_database" {
  name        = "sunat_ssco_perf"
  description = "Base de datos Glue para tablas generadas en experimentos de rendimiento (3.6 layouts P y N)."
}

resource "aws_iam_role" "glue_perf_crawler_role" {
  name = "sunat-ssco-perf-glue-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_perf_service_role" {
  role       = aws_iam_role.glue_perf_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_perf_s3_read" {
  name = "glue-perf-s3-read"
  role = aws_iam_role.glue_perf_crawler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.standard_perf["perf-36p"].arn,
          aws_s3_bucket.standard_perf["perf-36n"].arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = [
          "${aws_s3_bucket.standard_perf["perf-36p"].arn}/*",
          "${aws_s3_bucket.standard_perf["perf-36n"].arn}/*"
        ]
      }
    ]
  })
}

resource "aws_glue_crawler" "crawler_36p" {
  name          = "sunat-ssco-perf-36p-crawler"
  database_name = aws_glue_catalog_database.perf_database.name
  role          = aws_iam_role.glue_perf_crawler_role.arn
  table_prefix  = "p_"

  s3_target {
    path       = "s3://${aws_s3_bucket.standard_perf["perf-36p"].bucket}/silver/"
    exclusions = ["_markers/**", "**/_markers/**"]
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

resource "aws_glue_crawler" "crawler_36n" {
  name          = "sunat-ssco-perf-36n-crawler"
  database_name = aws_glue_catalog_database.perf_database.name
  role          = aws_iam_role.glue_perf_crawler_role.arn
  table_prefix  = "n_"

  s3_target {
    path       = "s3://${aws_s3_bucket.standard_perf["perf-36n"].bucket}/silver/"
    exclusions = ["_markers/**", "**/_markers/**"]
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

############ Presupuesto AWS (US$ 100) ############

resource "aws_budgets_budget" "perf_budget" {
  name              = "sunat-ssco-perf-budget-100usd"
  budget_type       = "COST"
  limit_amount      = var.budget_limit_amount
  limit_unit        = "USD"
  time_unit         = "MONTHLY"
  time_period_start = "2026-01-01_00:00"

  cost_filter {
    name = "TagKeyValue"
    values = [
      "user:Project$${var.project_tag}"
    ]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = length(var.budget_notification_emails) > 0 ? var.budget_notification_emails : ["admin@example.com"]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = length(var.budget_notification_emails) > 0 ? var.budget_notification_emails : ["admin@example.com"]
  }
}
