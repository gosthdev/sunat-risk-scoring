resource "aws_iam_role_policy" "deploy_jobs" {
  name = "deploy-jobs"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ProjectBucketsAccess"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketTagging"
        ]
        Resource = compact([
          var.artifacts_bucket_arn,
          var.raw_bucket_arn,
          var.bronze_bucket_arn,
          var.silver_bucket_arn,
          var.gold_bucket_arn
        ])
      },
      {
        Sid    = "ProjectObjectsAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetObjectTagging",
          "s3:PutObjectTagging",
          "s3:DeleteObjectTagging",
          "s3:GetObjectVersion",
          "s3:DeleteObjectVersion",
          "s3:GetObjectVersionTagging",
          "s3:PutObjectVersionTagging"
        ]
        Resource = compact(flatten([
          "${var.artifacts_bucket_arn}/*",
          "${var.raw_bucket_arn}/*",
          var.bronze_bucket_arn != "" ? ["${var.bronze_bucket_arn}/*"] : [],
          "${var.silver_bucket_arn}/*",
          var.gold_bucket_arn != "" ? ["${var.gold_bucket_arn}/*"] : []
        ]))
      },
      {
        Sid    = "ManageEphemeralEC2"
        Effect = "Allow"
        Action = [
          "ec2:Describe*",
          "ec2:CreateSecurityGroup",
          "ec2:DeleteSecurityGroup",
          "ec2:AuthorizeSecurityGroupIngress",
          "ec2:AuthorizeSecurityGroupEgress",
          "ec2:RevokeSecurityGroupIngress",
          "ec2:RevokeSecurityGroupEgress",
          "ec2:RunInstances",
          "ec2:TerminateInstances",
          "ec2:CreateTags",
          "ec2:DeleteTags"
        ]
        Resource = ["*"]
      },
      {
        Sid    = "ManageIngestionIAM"
        Effect = "Allow"
        Action = [
          "iam:CreateRole",
          "iam:DeleteRole",
          "iam:GetRole",
          "iam:UpdateRole",
          "iam:TagRole",
          "iam:UntagRole",
          "iam:ListRoleTags",
          "iam:ListRolePolicies",
          "iam:GetRolePolicy",
          "iam:PutRolePolicy",
          "iam:DeleteRolePolicy",
          "iam:AttachRolePolicy",
          "iam:DetachRolePolicy",
          "iam:ListAttachedRolePolicies",
          "iam:ListInstanceProfilesForRole",
          "iam:CreatePolicy",
          "iam:DeletePolicy",
          "iam:GetPolicy",
          "iam:GetPolicyVersion",
          "iam:CreatePolicyVersion",
          "iam:DeletePolicyVersion",
          "iam:ListPolicyVersions",
          "iam:TagPolicy",
          "iam:UntagPolicy",
          "iam:ListPolicyTags",
          "iam:CreateInstanceProfile",
          "iam:DeleteInstanceProfile",
          "iam:GetInstanceProfile",
          "iam:AddRoleToInstanceProfile",
          "iam:RemoveRoleFromInstanceProfile",
          "iam:TagInstanceProfile",
          "iam:UntagInstanceProfile",
          "iam:ListInstanceProfileTags"
        ]
        Resource = [
          "arn:aws:iam::*:role/sunat-ingestion-ec2-role",
          "arn:aws:iam::*:policy/sunat-ingestion-ec2-s3-policy",
          "arn:aws:iam::*:instance-profile/sunat-ingestion-ec2-profile"
        ]
      },
      {
        Sid      = "PassIngestionRoleToEC2"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = ["arn:aws:iam::*:role/sunat-ingestion-ec2-role"]
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "ec2.amazonaws.com"
          }
        }
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
      },
      {
        Sid    = "GlueAndAthenaAnalytics"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:CreateDatabase",
          "glue:GetCrawler",
          "glue:CreateCrawler",
          "glue:UpdateCrawler",
          "glue:StartCrawler",
          "glue:GetTables",
          "glue:GetTable",
          "athena:GetWorkGroup",
          "athena:CreateWorkGroup",
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution"
        ]
        Resource = ["*"]
      }
    ]
  })
}
