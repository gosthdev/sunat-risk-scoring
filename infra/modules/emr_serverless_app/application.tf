resource "aws_emrserverless_application" "this" {
  name          = var.application_name
  release_label = var.release_label
  type          = "spark"
  architecture  = "X86_64"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.idle_timeout_minutes
  }

  maximum_capacity {
    cpu    = var.max_cpu
    memory = var.max_memory
    disk   = var.max_disk
  }
}
