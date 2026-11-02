provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = {
      Project   = var.project_tag
      Component = "ephemeral-ingestion-ec2"
      ManagedBy = "terraform"
    }
  }
}
