resource "aws_glue_job" "extract_jobs" {
  for_each = var.extract_jobs

  name         = "${var.project_name}-extract-${each.key}"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket_name}/assets/extract_jobs/${each.value.script}"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-job-insights"        = "true"
    "--job-language"               = "python"
    "--bronze_bucket"              = var.bronze_bucket_name
    "--ingest_date"                = var.ingest_date
    "--additional-python-modules"  = join(",", [
      for whl in fileset("${path.module}/../../assets/dependencies", "*.whl") :
      "s3://${var.scripts_bucket_name}/assets/dependencies/${whl}"
    ])
  }

  timeout           = each.value.timeout
  number_of_workers = 2
  worker_type       = "G.1X"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Job         = each.key
  }

  depends_on = [aws_s3_object.dependencies]
}

# Upload extract scripts to S3
resource "aws_s3_object" "extract_scripts" {
  for_each = var.extract_jobs

  bucket = var.scripts_bucket_name
  key    = "assets/extract_jobs/${each.value.script}"
  source = "${path.module}/../../assets/extract_jobs/${each.value.script}"
  etag   = filemd5("${path.module}/../../assets/extract_jobs/${each.value.script}")

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# Upload all wheel files to S3
resource "aws_s3_object" "dependencies" {
  for_each = fileset("${path.module}/../../assets/dependencies", "*.whl")

  bucket      = var.scripts_bucket_name
  key         = "assets/dependencies/${each.value}"
  source      = "${path.module}/../../assets/dependencies/${each.value}"
  source_hash = filemd5("${path.module}/../../assets/dependencies/${each.value}")

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}