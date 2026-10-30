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
        # Lectura y escritura de objetos; sin borrado (el borrado va aparte).
        Sid    = "ProjectObjectsReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
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
        # clean_layer.sh borra solo prefijos de bronze, silver y gold.
        Sid    = "CleanProcessedLayers"
        Effect = "Allow"
        Action = ["s3:DeleteObject"]
        Resource = compact(flatten([
          var.bronze_bucket_arn != "" ? ["${var.bronze_bucket_arn}/*"] : [],
          "${var.silver_bucket_arn}/*",
          var.gold_bucket_arn != "" ? ["${var.gold_bucket_arn}/*"] : []
        ]))
      },
      {
        # Solo los marcadores de ingesta que run_and_destroy.sh borra en la raíz de raw.
        Sid      = "DeleteRawIngestionFlags"
        Effect   = "Allow"
        Action   = ["s3:DeleteObject"]
        Resource = ["${var.raw_bucket_arn}/_INGESTION_*"]
      },
      {
        # El provider de infra/ingestion_ec2 etiqueta estos objetos (default_tags).
        Sid      = "TagIngestionBootstrap"
        Effect   = "Allow"
        Action   = ["s3:GetObjectTagging", "s3:PutObjectTagging"]
        Resource = ["${var.raw_bucket_arn}/_bootstrap/*"]
      },
      {
        # shortcut: RunInstances y CreateSecurityGroup sin condición de tag: el
        # ARN de instancias/SG no se conoce de antemano. Upgrade: etiquetar en
        # creación y condicionar por ec2:ResourceTag.
        Sid    = "ManageEphemeralEC2"
        Effect = "Allow"
        Action = [
          "ec2:Describe*",
          "ec2:CreateSecurityGroup",
          "ec2:DeleteSecurityGroup",
          "ec2:AuthorizeSecurityGroupEgress",
          "ec2:RevokeSecurityGroupEgress",
          "ec2:RunInstances",
          "ec2:CreateTags"
        ]
        Resource = ["*"]
      },
      {
        # Solo la EC2 de ingesta.
        Sid      = "TerminateIngestionWorker"
        Effect   = "Allow"
        Action   = ["ec2:TerminateInstances"]
        Resource = ["*"]
        Condition = {
          StringEquals = {
            "ec2:ResourceTag/Name" = "sunat-ingestion-worker"
          }
        }
      },
      {
        # Sin Attach/Detach ni Put/DeleteRolePolicy: el rol de ingesta solo usa
        # políticas administradas (ver AttachIngestionManagedPolicies).
        Sid    = "ManageIngestionIAM"
        Effect = "Allow"
        Action = [
          "iam:DeleteRole",
          "iam:GetRole",
          "iam:UpdateRole",
          "iam:TagRole",
          "iam:UntagRole",
          "iam:ListRoleTags",
          "iam:ListRolePolicies",
          "iam:GetRolePolicy",
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
        # El rol de ingesta solo puede crearse con el boundary de services.
        Sid      = "CreateIngestionRoleWithBoundary"
        Effect   = "Allow"
        Action   = ["iam:CreateRole"]
        Resource = ["arn:aws:iam::*:role/sunat-ingestion-ec2-role"]
        Condition = {
          StringLike = {
            "iam:PermissionsBoundary" = "arn:aws:iam::*:policy/sunat-ingestion-ec2-boundary"
          }
        }
      },
      {
        # Solo políticas administradas ya conocidas: impide adjuntar AdministratorAccess.
        Sid    = "AttachIngestionManagedPolicies"
        Effect = "Allow"
        Action = [
          "iam:AttachRolePolicy",
          "iam:DetachRolePolicy"
        ]
        Resource = ["arn:aws:iam::*:role/sunat-ingestion-ec2-role"]
        Condition = {
          ArnLike = {
            "iam:PolicyARN" = [
              "arn:aws:iam::*:policy/sunat-ingestion-ec2-s3-policy",
              "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
            ]
          }
        }
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
        Sid      = "ReadPublicAmiParameters"
        Effect   = "Allow"
        Action   = ["ssm:GetParameter"]
        Resource = ["arn:aws:ssm:*::parameter/aws/service/ami-amazon-linux-latest/*"]
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
        Sid      = "PassGlueCrawlerRole"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [var.glue_crawler_role_arn != "" ? var.glue_crawler_role_arn : "arn:aws:iam::*:role/sunat-ssco-glue-crawler-role"]
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "glue.amazonaws.com"
          }
        }
      },
      {
        # Catálogo: bases sunat_ssco y ssco_catalog (nombre legacy que aún chequea verify_pipeline_health.sh).
        Sid    = "GlueCatalogAnalytics"
        Effect = "Allow"
        # GetTable y las de particiones las usa Athena al leer el catálogo con este rol.
        Action = [
          "glue:GetDatabase",
          "glue:GetTables",
          "glue:GetTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition"
        ]
        Resource = [
          "arn:aws:glue:*:*:catalog",
          "arn:aws:glue:*:*:database/sunat_ssco",
          "arn:aws:glue:*:*:database/ssco_catalog",
          "arn:aws:glue:*:*:table/sunat_ssco/*",
          "arn:aws:glue:*:*:table/ssco_catalog/*"
        ]
      },
      {
        # Terraform crea y configura los crawlers; CD solo los consulta y los ejecuta (06_create_glue_crawler.sh).
        Sid    = "GlueCrawlers"
        Effect = "Allow"
        Action = [
          "glue:GetCrawler",
          "glue:StartCrawler"
        ]
        Resource = [
          "arn:aws:glue:*:*:crawler/sunat-ssco-*"
        ]
      },
      {
        Sid    = "AthenaWorkgroup"
        Effect = "Allow"
        Action = [
          "athena:GetWorkGroup",
          "athena:CreateWorkGroup",
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetDataCatalog"
        ]
        Resource = [
          "arn:aws:athena:*:*:workgroup/sunat-ssco",
          "arn:aws:athena:*:*:workgroup/ssco-workgroup",
          "arn:aws:athena:*:*:datacatalog/*"
        ]
      },
      {
        # Solo los jobs de train_models: run_id = ssco-all-<tag> (launch_manual.py).
        Sid    = "SageMakerTrainingJobsAccess"
        Effect = "Allow"
        Action = [
          "sagemaker:CreateTrainingJob",
          "sagemaker:DescribeTrainingJob"
        ]
        Resource = ["arn:aws:sagemaker:*:*:training-job/ssco-all-*"]
      },
      {
        Sid      = "PassSageMakerExecutionRole"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [var.sagemaker_execution_role_arn != "" ? var.sagemaker_execution_role_arn : "arn:aws:iam::*:role/sunat-ssco-sagemaker-execution-role"]
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "sagemaker.amazonaws.com"
          }
        }
      }
    ]
  })
}
