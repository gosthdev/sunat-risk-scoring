resource "aws_athena_workgroup" "this" {
  name          = var.workgroup_name
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = var.bytes_scanned_cutoff_per_query

    result_configuration {
      output_location = "s3://${var.results_bucket_name}/athena-results/"
    }
  }
}
