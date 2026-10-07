aws_region  = "us-east-1"
project_tag = "sunat-risk-scoring"

raw_bucket_name       = "sunat-risk-scoring-raw"
bronze_bucket_name    = "sunat-risk-scoring-bronze"
silver_bucket_name    = "sunat-risk-scoring-silver"
gold_bucket_name      = "sunat-risk-scoring-gold"
artifacts_bucket_name = "sunat-risk-scoring-artifacts"

# Mantener en false para proteger los buckets contra borrado accidental.
force_destroy_buckets = false
