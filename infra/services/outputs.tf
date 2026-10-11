output "emr_application_id" {
  value       = module.emr_serverless_app.application_id
  description = "ID de la aplicación EMR Serverless."
}

output "emr_execution_role_arn" {
  value       = module.iam_emr_execution_role.role_arn
  description = "ARN del rol de ejecución IAM para EMR Serverless."
}

output "glue_database" {
  value       = module.glue_database.database_name
  description = "Nombre del catálogo de base de datos AWS Glue."
}

output "glue_crawler_silver" {
  value       = module.glue_crawler_silver.crawler_name
  description = "Nombre del crawler Glue para la zona Silver."
}

output "glue_crawler_gold" {
  value       = module.glue_crawler_gold.crawler_name
  description = "Nombre del crawler Glue para la zona Gold."
}

output "athena_workgroup" {
  value       = module.athena_workgroup.workgroup_name
  description = "Nombre del Workgroup de Athena."
}

output "github_actions_role_arn" {
  value       = module.iam_github_actions_role.role_arn
  description = "ARN del rol OIDC para GitHub Actions."
}

output "glue_crawler_role_arn" {
  value       = module.iam_glue_crawler_role.role_arn
  description = "ARN del rol de ejecución IAM para Glue Crawler."
}

output "sagemaker_execution_role_arn" {
  value       = module.iam_sagemaker_execution_role.role_arn
  description = "ARN del rol de ejecución IAM para SageMaker Training Jobs."
}

output "train_ec2_role_arn" {
  value       = module.iam_train_ec2_role.role_arn
  description = "ARN del rol IAM de la EC2 efímera de entrenamiento."
}

output "train_ec2_instance_profile_name" {
  value       = module.iam_train_ec2_role.instance_profile_name
  description = "Perfil de instancia que usa scripts/train_ec2.sh."
}
