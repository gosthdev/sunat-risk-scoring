resource "aws_glue_crawler" "this" {
  name          = var.crawler_name
  database_name = var.database_name
  role          = var.role_arn
  table_prefix  = var.table_prefix

  s3_target {
    path = var.s3_target_path
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}
