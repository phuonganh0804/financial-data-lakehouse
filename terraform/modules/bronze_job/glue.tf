resource "aws_glue_job" "bronze_jobs" {
  for_each = var.bronze_jobs

  name         = "${var.project_name}-bronze-${each.key}"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  # Structuring raw landing payloads into columnar Parquet is a transform, so
  # it runs on Spark — but it reads from the immutable landing zone, never the
  # source API, so bronze can always be rebuilt by replaying landing.
  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket_name}/assets/bronze_jobs/${each.value.script}"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-job-insights"      = "true"
    "--job-language"             = "python"
    "--enable-glue-datacatalog"  = "true"
    "--landing_bucket"           = var.landing_bucket_name
    "--bronze_bucket"            = var.bronze_bucket_name
    "--ingest_date"              = var.ingest_date
    "--interval"                 = var.interval
    "--api_start_date"           = var.api_start_date
    "--api_end_date"             = var.api_end_date
    "--macro_series_config_path" = var.macro_series_config_path
  }

  timeout           = each.value.timeout
  number_of_workers = 2
  worker_type       = "G.1X"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Job         = each.key
    Stage       = "bronze"
  }
}

resource "aws_s3_object" "bronze_scripts" {
  for_each = var.bronze_jobs

  bucket = var.scripts_bucket_name
  key    = "assets/bronze_jobs/${each.value.script}"
  source = "${path.module}/../../assets/bronze_jobs/${each.value.script}"
  etag   = filemd5("${path.module}/../../assets/bronze_jobs/${each.value.script}")

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
