provider "aws" {
  region  = var.aws_region
  profile = "bigdata"

  default_tags {
    tags = {
      Project   = var.project_tag
      ManagedBy = "terraform"
    }
  }
}