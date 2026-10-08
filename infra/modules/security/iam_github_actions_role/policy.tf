resource "aws_iam_role_policy" "deploy_jobs" {
  name = "deploy-jobs"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ArtifactsBucketAccess"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [var.artifacts_bucket_arn]
      },
      {
        Sid    = "UploadJobsAndArtifacts"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ]
        Resource = ["${var.artifacts_bucket_arn}/*"]
      },
      {
        Sid    = "DataBucketsAccess"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          var.raw_bucket_arn,
          var.silver_bucket_arn
        ]
      },
      {
        Sid      = "ReadRawForSmallDatasets"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${var.raw_bucket_arn}/*"]
      },
      {
        Sid    = "ReadWriteSilverForSmallDatasets"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = ["${var.silver_bucket_arn}/*"]
      },
      {
        Sid    = "EMRServerlessListApplications"
        Effect = "Allow"
        Action = [
          "emr-serverless:ListApplications",
          "emr-serverless:GetApplication"
        ]
        Resource = ["*"]
      },
      {
        Sid    = "EMRServerlessManageJobRuns"
        Effect = "Allow"
        Action = [
          "emr-serverless:StartJobRun",
          "emr-serverless:GetJobRun",
          "emr-serverless:CancelJobRun",
          "emr-serverless:ListJobRuns"
        ]
        Resource = [
          var.emr_application_arn,
          "${var.emr_application_arn}/jobruns/*"
        ]
      },
      {
        Sid      = "PassExecutionRoleToEMR"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [var.emr_execution_role_arn]
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "emr-serverless.amazonaws.com"
          }
        }
      }
    ]
  })
}
