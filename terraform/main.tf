locals {
  transform_jobs = {
    "binance" = {
      script     = "transform_binance_historical.py"
      table_name = "binance_klines"
      timeout    = 10
    }
    "twelvedata" = {
      script     = "transform_twelvedata_equities.py"
      table_name = "equity_prices"
      timeout    = 10
    }
    "fred" = {
      script     = "transform_fred_macro.py"
      table_name = "fred_macro"
      timeout    = 10
    }
  }

  # Landing: Python Shell jobs that pull APIs and write raw responses
  # byte-for-byte to the immutable landing zone (no Spark).
  landing_jobs = {
    "binance" = {
      script  = "landing_binance.py"
      timeout = 5
    }
    "twelvedata" = {
      script  = "landing_twelvedata.py"
      timeout = 5
    }
    "fred" = {
      script  = "landing_fred.py"
      timeout = 5
    }
  }

  # Bronze: Spark jobs that parse the raw landing payloads into columnar
  # Parquet. They read from landing, never the source API.
  bronze_jobs = {
    "binance" = {
      script  = "bronze_binance.py"
      timeout = 10
    }
    "twelvedata" = {
      script  = "bronze_twelvedata.py"
      timeout = 10
    }
    "fred" = {
      script  = "bronze_fred.py"
      timeout = 10
    }
  }
}


module "landing_job" {
  source = "./modules/landing_job"

  project_name             = var.project_name
  environment              = var.environment
  landing_bucket_name      = aws_s3_bucket.landing.id
  landing_bucket_arn       = aws_s3_bucket.landing.arn
  scripts_bucket_name      = aws_s3_bucket.scripts.id
  scripts_bucket_arn       = aws_s3_bucket.scripts.arn
  ingest_date              = var.ingest_date
  api_start_date           = var.api_start_date
  api_end_date             = var.api_end_date
  interval                 = var.interval
  ticker_config_path       = "s3://${aws_s3_bucket.scripts.id}/config/equity_tickers.json"
  macro_series_config_path = "s3://${aws_s3_bucket.scripts.id}/config/macro_series.json"
  landing_jobs             = local.landing_jobs
}

module "bronze_job" {
  source = "./modules/bronze_job"

  project_name             = var.project_name
  environment              = var.environment
  landing_bucket_name      = aws_s3_bucket.landing.id
  landing_bucket_arn       = aws_s3_bucket.landing.arn
  bronze_bucket_name       = aws_s3_bucket.bronze.id
  bronze_bucket_arn        = aws_s3_bucket.bronze.arn
  scripts_bucket_name      = aws_s3_bucket.scripts.id
  scripts_bucket_arn       = aws_s3_bucket.scripts.arn
  ingest_date              = var.ingest_date
  api_start_date           = var.api_start_date
  api_end_date             = var.api_end_date
  interval                 = var.interval
  macro_series_config_path = "s3://${aws_s3_bucket.scripts.id}/config/macro_series.json"
  bronze_jobs              = local.bronze_jobs
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

# Single source of truth for the FRED macro series, read at runtime by both
# the landing job (fetch loop + failure audit) and the bronze job (metadata
# enrichment) — so the two can never drift.
resource "aws_s3_object" "macro_series_config" {
  bucket = aws_s3_bucket.scripts.id
  key    = "config/macro_series.json"
  source = "${path.module}/macro_series.json"
  etag   = filemd5("${path.module}/macro_series.json")

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

module "serving" {
  source = "./modules/serving"

  project_name     = var.project_name
  environment      = var.environment
  gold_bucket_name = aws_s3_bucket.gold.id
  gold_database    = "financial_data_lakehouse_gold"
}

module "data_quality" {
  source = "./modules/data_quality"
  # Standalone DQDL rulesets — no inputs, no dependencies. The silver table is
  # bound at evaluation time (Airflow DQ task), not here.
}