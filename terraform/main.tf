locals {
  transform_jobs = {
    "binance" = {
      script     = "transform_binance_historical.py"
      table_name = "binance_klines"
      timeout    = 10
    }
  }

  extract_jobs = {
    "binance" = {
      script  = "binance_historical.py"
      timeout = 5
    }
    "twelvedata" = {
      script  = "twelvedata_equities.py"
      timeout = 5
    }
    "fred" = {
      script  = "fred_macro.py"
      timeout = 5
    }
  }
}

module "transform_job" {
  source = "./modules/transform_job"

  project_name        = var.project_name
  environment         = var.environment
  bronze_bucket_name  = aws_s3_bucket.bronze.id
  bronze_bucket_arn   = aws_s3_bucket.bronze.arn
  silver_bucket_name  = aws_s3_bucket.silver.id
  silver_bucket_arn   = aws_s3_bucket.silver.arn
  scripts_bucket_name = aws_s3_bucket.scripts.id
  scripts_bucket_arn  = aws_s3_bucket.scripts.arn
  catalog_database    = var.catalog_database
  ingest_date         = var.ingest_date
  interval            = var.interval
  transform_jobs      = local.transform_jobs
}

resource "aws_s3_object" "ticker_config" {
  bucket = aws_s3_bucket.scripts.id
  key    = "config/equity_tickers.json"
  source = "${path.module}/equity_tickers.json"
  etag   = filemd5("${path.module}/equity_tickers.json")

  tags = {
    Project     = var.project_name
    Environment = var.environment
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
  ticker_config_path  = "s3://${aws_s3_bucket.scripts.id}/config/equity_tickers.json"
  extract_jobs        = local.extract_jobs
}