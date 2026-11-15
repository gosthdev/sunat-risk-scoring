locals {
  account_id = data.aws_caller_identity.current.account_id
  glue_arn   = "arn:aws:glue:${var.aws_region}:${local.account_id}"
  athena_arn = "arn:aws:athena:${var.aws_region}:${local.account_id}"

  # Sin prefijo gold/: con buckets separados las claves van en la raíz del bucket gold.
  # scoring_dataset queda fuera a propósito.
  gold_read_prefixes = [
    "regional_summary",
    "ruc_features",
    "model_outputs/predictions",
    "model_runs",
    "model_metrics",
  ]
}

resource "aws_iam_role_policy" "analytics_read" {
  name = "quicksight-athena-glue-s3-read"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaQueryWorkgroup"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:StopQueryExecution",
          "athena:GetQueryExecution",
          "athena:BatchGetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetQueryResultsStream",
          "athena:GetWorkGroup"
        ]
        Resource = ["${local.athena_arn}:workgroup/${var.workgroup_name}"]
      },
      {
        Sid    = "AthenaCatalogMetadata"
        Effect = "Allow"
        Action = [
          "athena:GetDataCatalog",
          "athena:GetDatabase",
          "athena:GetTableMetadata",
          "athena:ListDatabases",
          "athena:ListTableMetadata"
        ]
        Resource = ["${local.athena_arn}:datacatalog/AwsDataCatalog"]
      },
      {
        # Estas dos acciones de Athena no admiten permisos a nivel de recurso.
        Sid      = "AthenaListNoResourceLevel"
        Effect   = "Allow"
        Action   = ["athena:ListDataCatalogs", "athena:ListWorkGroups"]
        Resource = ["*"]
      },
      {
        Sid    = "GlueCatalogRead"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition"
        ]
        Resource = [
          "${local.glue_arn}:catalog",
          "${local.glue_arn}:database/${var.glue_database_name}",
          "${local.glue_arn}:table/${var.glue_database_name}/*"
        ]
      },
      {
        Sid      = "GoldBucketLocation"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation"]
        Resource = [var.gold_bucket_arn]
      },
      {
        Sid      = "GoldListPublishedPrefixes"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [var.gold_bucket_arn]
        Condition = {
          StringLike = {
            "s3:prefix" = flatten([for p in local.gold_read_prefixes : [p, "${p}/*"]])
          }
        }
      },
      {
        Sid      = "GoldReadPublishedPrefixes"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = [for p in local.gold_read_prefixes : "${var.gold_bucket_arn}/${p}/*"]
      },
      {
        # ListBucketMultipartUploads es de bucket y no admite condición s3:prefix.
        Sid      = "AthenaResultsBucketLevel"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
        Resource = [var.artifacts_bucket_arn]
      },
      {
        Sid      = "AthenaResultsList"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [var.artifacts_bucket_arn]
        Condition = {
          StringLike = { "s3:prefix" = ["athena-results", "athena-results/*"] }
        }
      },
      {
        Sid    = "AthenaResultsReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = ["${var.artifacts_bucket_arn}/athena-results/*"]
      }
    ]
  })
}
