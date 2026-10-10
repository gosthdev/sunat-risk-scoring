resource "aws_iam_role_policy" "s3_access" {
  name = "s3-access"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListBuckets"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads"
        ]
        Resource = [
          var.raw_bucket_arn,
          var.bronze_bucket_arn,
          var.silver_bucket_arn,
          var.gold_bucket_arn,
          var.artifacts_bucket_arn
        ]
      },
      {
        Sid      = "ReadRaw"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${var.raw_bucket_arn}/*"]
      },
      {
        Sid    = "ReadWriteBronzeSilver"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = [
          "${var.bronze_bucket_arn}/*",
          "${var.silver_bucket_arn}/*"
        ]
      },
      {
        Sid    = "ReadGold"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = [
          "${var.gold_bucket_arn}/*"
        ]
      },
      {
        Sid    = "WriteGoldAllowedPrefixes"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = [
          "${var.gold_bucket_arn}/gold/ruc_features/*",
          "${var.gold_bucket_arn}/gold/regional_summary/*",
          "${var.gold_bucket_arn}/gold/scoring_dataset/*",
          "${var.gold_bucket_arn}/gold/model_inputs/*"
        ]
      },
      {
        Sid      = "ReadJobs"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${var.artifacts_bucket_arn}/jobs/*"]
      },
      {
        Sid    = "WriteLogs"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload"
        ]
        Resource = ["${var.artifacts_bucket_arn}/emr-logs/*"]
      }
    ]
  })
}
