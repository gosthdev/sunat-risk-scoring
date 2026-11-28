# La access key se crea a mano (no vive en el estado de Terraform).
resource "aws_iam_user" "this" {
  name = var.user_name
}

resource "aws_iam_user_policy" "inbox_upload" {
  name = "inbox-upload"
  user = aws_iam_user.this.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "PutInbox"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${var.raw_bucket_arn}/_inbox/*"]
      },
      {
        Sid      = "DenyPutRechazados"
        Effect   = "Deny"
        Action   = ["s3:PutObject"]
        Resource = ["${var.raw_bucket_arn}/_inbox/_rechazados/*"]
      }
    ]
  })
}
