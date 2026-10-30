terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  backend "s3" {
    bucket       = "sunat-risk-scoring-tfstate"
    key          = "storage/terraform.tfstate"
    region       = "us-east-1"
    profile      = "bigdata"
    encrypt      = true
    use_lockfile = true
  }
}
