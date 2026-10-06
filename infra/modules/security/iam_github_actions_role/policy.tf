resource "aws_iam_role_policy" "deploy_jobs" {
  name = "deploy-jobs"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListArtifactsBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [var.artifacts_bucket_arn]
      },
      {
        Sid      = "UploadJobs"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${var.artifacts_bucket_arn}/jobs/*"]
      }
    ]
  })
}
