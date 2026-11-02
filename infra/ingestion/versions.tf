terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Sin profile: en local lo da AWS_PROFILE (run_and_destroy.sh) y en CI el OIDC.
  backend "s3" {
    bucket       = "sunat-risk-scoring-tfstate"
    key          = "ingestion/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}
