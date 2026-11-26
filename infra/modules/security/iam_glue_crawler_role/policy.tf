resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "s3_read" {
  name = "s3-read-silver-gold"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [var.silver_bucket_arn, var.gold_bucket_arn]
      },
      {
        Sid      = "ListBronzePendientes"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [var.bronze_bucket_arn]
        Condition = {
          StringLike = { "s3:prefix" = ["_pendientes", "_pendientes/*"] }
        }
      },
      {
        Sid      = "ReadBronzePendientes"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${var.bronze_bucket_arn}/_pendientes/*"]
      },
      {
        Sid      = "ReadObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${var.silver_bucket_arn}/*", "${var.gold_bucket_arn}/*"]
      }
    ]
  })
}
