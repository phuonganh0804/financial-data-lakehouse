locals {
  extract_jobs = {
    "binance" = {
      script  = "binance_historical.py"
      timeout = 10
    }
    "yfinance" = {
      script  = "yfinance_dax.py"
      timeout = 10
    }
    "fred" = {
      script  = "fred_macro.py"
      timeout = 10
    }
  }
}

module "extract_job" {
  source = "./modules/extract_job"

  project_name        = var.project_name
  environment         = var.environment
  bronze_bucket_name  = aws_s3_bucket.bronze.id
  bronze_bucket_arn   = aws_s3_bucket.bronze.arn
  scripts_bucket_name = aws_s3_bucket.scripts.id
  scripts_bucket_arn  = aws_s3_bucket.scripts.arn
  ingest_date         = "2024-01-01"
  extract_jobs        = local.extract_jobs
}