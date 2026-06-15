resource "aws_glue_job" "landing_jobs" {
  for_each = var.landing_jobs

  name     = "${var.project_name}-landing-${each.key}"
  role_arn = aws_iam_role.glue_role.arn

  # Ingestion is I/O-bound API pulling — no distributed compute needed.
  # Python Shell runs on a single lightweight node, a fraction of the cost of
  # a Glue Spark (glueetl) job. These jobs only fetch and write raw responses
  # byte-for-byte to the landing zone; the bronze Spark jobs do the parsing.
  command {
    name            = "pythonshell"
    script_location = "s3://${var.scripts_bucket_name}/assets/landing_jobs/${each.value.script}"
    python_version  = "3.9"
  }

  default_arguments = {
    "--job-language"              = "python"
    "--additional-python-modules" = "requests"
    "--landing_bucket"            = var.landing_bucket_name
    "--ingest_date"               = var.ingest_date
    "--api_start_date"            = var.api_start_date
    "--api_end_date"              = var.api_end_date
    "--interval"                  = var.interval
    "--ticker_config_path"        = var.ticker_config_path
    "--macro_series_config_path"  = var.macro_series_config_path
  }

  timeout      = each.value.timeout
  max_capacity = 1.0

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Job         = each.key
    Stage       = "landing"
  }
}

resource "aws_s3_object" "landing_scripts" {
  for_each = var.landing_jobs

  bucket = var.scripts_bucket_name
  key    = "assets/landing_jobs/${each.value.script}"
  source = "${path.module}/../../assets/landing_jobs/${each.value.script}"
  etag   = filemd5("${path.module}/../../assets/landing_jobs/${each.value.script}")

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
