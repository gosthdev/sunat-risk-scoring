# Tope de permisos del rol de ingesta. Lo aplica services (el usuario); CI solo
# lo referencia en CreateRole y no puede modificarlo. Con el boundary, aunque CI
# cambie la política S3 a una versión administrativa, el rol sigue acotado aquí.
resource "aws_iam_policy" "ingestion_boundary" {
  name        = "sunat-ingestion-ec2-boundary"
  description = "Tope de permisos de la EC2 de ingesta: raw (S3) y agente SSM."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RawBucketOnly"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          var.raw_bucket_arn,
          "${var.raw_bucket_arn}/*"
        ]
      },
      {
        Sid    = "SSMCore"
        Effect = "Allow"
        Action = [
          "ssm:DescribeAssociation",
          "ssm:GetDeployablePatchSnapshotForInstance",
          "ssm:GetDocument",
          "ssm:DescribeDocument",
          "ssm:GetManifest",
          "ssm:GetParameter",
          "ssm:GetParameters",
          "ssm:ListAssociations",
          "ssm:ListInstanceAssociations",
          "ssm:PutInventory",
          "ssm:PutComplianceItems",
          "ssm:PutConfigurePackageResult",
          "ssm:UpdateAssociationStatus",
          "ssm:UpdateInstanceAssociationStatus",
          "ssm:UpdateInstanceInformation",
          "ssmmessages:CreateControlChannel",
          "ssmmessages:CreateDataChannel",
          "ssmmessages:OpenControlChannel",
          "ssmmessages:OpenDataChannel",
          "ec2messages:AcknowledgeMessage",
          "ec2messages:DeleteMessage",
          "ec2messages:FailMessage",
          "ec2messages:GetEndpoint",
          "ec2messages:GetMessages",
          "ec2messages:SendReply"
        ]
        Resource = ["*"]
      }
    ]
  })
}
