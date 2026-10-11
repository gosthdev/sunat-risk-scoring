resource "aws_iam_role_policy" "s3_access" {
  name = "sagemaker-s3-access"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListGoldBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [var.gold_bucket_arn]
      },
      {
        Sid    = "ReadModelInputs"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = [
          "${var.gold_bucket_arn}/model_inputs/*"
        ]
      },
      {
        Sid    = "WriteModelArtifactsAndOutputs"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = [
          "${var.gold_bucket_arn}/model_outputs/*",
          "${var.gold_bucket_arn}/model_artifacts/*",
          "${var.gold_bucket_arn}/model_runs/*",
          "${var.gold_bucket_arn}/model_metrics/*",
          "${var.gold_bucket_arn}/model_leaderboards/*"
        ]
      },
      {
        Sid    = "ReadCodeArtifacts"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = ["${var.gold_bucket_arn}/code/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy" "cloudwatch_logs" {
  name = "sagemaker-cloudwatch-logs"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogsAccess"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:*:*:log-group:/aws/sagemaker/*",
          "arn:aws:logs:*:*:log-group:/aws/sagemaker/TrainingJobs:*"
        ]
      }
    ]
  })
}
