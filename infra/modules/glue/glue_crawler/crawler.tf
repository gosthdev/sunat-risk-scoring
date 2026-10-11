resource "aws_glue_crawler" "this" {
  name          = var.crawler_name
  database_name = var.database_name
  role          = var.role_arn
  table_prefix  = var.table_prefix

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
    }
    Grouping = merge(
      { TableGroupingPolicy = "CombineCompatibleSchemas" },
      var.table_level == null ? {} : { TableLevelConfiguration = var.table_level }
    )
  })

  s3_target {
    path       = var.s3_target_path
    exclusions = concat(["_markers/**", "**/_markers/**"], var.extra_exclusions)
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}
