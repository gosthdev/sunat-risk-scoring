output "perf_buckets" {
  description = "Nombres de los buckets de pruebas de rendimiento creados."
  value = {
    perf_36p  = aws_s3_bucket.standard_perf["perf-36p"].bucket
    perf_36n  = aws_s3_bucket.standard_perf["perf-36n"].bucket
    perf_e2   = aws_s3_bucket.standard_perf["perf-e2"].bucket
    perf_x1   = aws_s3_bucket.standard_perf["perf-x1"].bucket
    perf_x10  = aws_s3_bucket.standard_perf["perf-x10"].bucket
    perf_x100 = aws_s3_bucket.perf_x100.bucket
  }
}

output "emr_perf_role_arn" {
  description = "ARN del rol de ejecución de EMR Serverless para pruebas."
  value       = aws_iam_role.emr_perf_role.arn
}

output "emr_perf_application_id" {
  description = "ID de la aplicación EMR Serverless para pruebas."
  value       = aws_emrserverless_application.perf_spark.id
}

output "emr_perf_application_arn" {
  description = "ARN de la aplicación EMR Serverless para pruebas."
  value       = aws_emrserverless_application.perf_spark.arn
}

output "glue_perf_database_name" {
  description = "Nombre de la base de datos Glue para pruebas."
  value       = aws_glue_catalog_database.perf_database.name
}

output "glue_perf_crawlers" {
  description = "Nombres de los crawlers Glue de rendimiento."
  value = {
    crawler_36p = aws_glue_crawler.crawler_36p.name
    crawler_36n = aws_glue_crawler.crawler_36n.name
  }
}

output "budget_name" {
  description = "Nombre del presupuesto AWS de pruebas."
  value       = aws_budgets_budget.perf_budget.name
}
