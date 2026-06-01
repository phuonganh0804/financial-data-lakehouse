locals {
  extract_jobs = {
    "binance" = {
      script  = "binance_historical.py"
      timeout = 5
    }
    "yfinance" = {
      script  = "yfinance_dax.py"
      timeout = 5
    }
    "fred" = {
      script  = "fred_macro.py"
      timeout = 5
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
  ingest_date         = var.ingest_date
  api_start_date      = var.api_start_date
  api_end_date        = var.api_end_date
  interval            = var.interval
  ticker_config_path  = "s3://${aws_s3_bucket.scripts.id}/config/dax40_tickers.json"
  extract_jobs        = local.extract_jobs
}