resource "aws_iam_role_policy" "s3_access" {
  name = "train-ec2-s3-access"
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
        Resource = ["${var.gold_bucket_arn}/model_inputs/*"]
      },
      {
        Sid    = "WriteTrainingOutputs"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = [
          "${var.gold_bucket_arn}/model_outputs/*",
          "${var.gold_bucket_arn}/model_artifacts/*",
          "${var.gold_bucket_arn}/model_runs/*",
          "${var.gold_bucket_arn}/model_leaderboards/*",
          "${var.gold_bucket_arn}/model_reports/*",
          "${var.gold_bucket_arn}/model_logs/*"
        ]
      },
      {
        Sid      = "ReadTrainingCode"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${var.artifacts_bucket_arn}/train_ec2/*"]
      }
    ]
  })
}
