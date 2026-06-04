resource "aws_glue_catalog_database" "silver" {
  name        = var.catalog_database
  description = "Glue Catalog database for transformations"
}

resource "aws_glue_job" "transform_jobs" {
  for_each = var.transform_jobs

  name         = "${var.project_name}-transform-${each.key}"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket_name}/assets/transform_jobs/${each.value.script}"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-job-insights"  = "true"
    "--job-language"         = "python"
    "--datalake-formats"     = "iceberg"
    "--bronze_bucket"        = var.bronze_bucket_name
    "--silver_bucket"        = var.silver_bucket_name
    "--catalog_database"     = var.catalog_database
    "--table_name"           = each.value.table_name
    "--ingest_date"          = var.ingest_date
    "--interval"             = var.interval
  }

  timeout           = each.value.timeout
  number_of_workers = 2
  worker_type       = "G.1X"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Job         = each.key
  }
}

resource "aws_s3_object" "transform_scripts" {
  for_each = var.transform_jobs

  bucket = var.scripts_bucket_name
  key    = "assets/transform_jobs/${each.value.script}"
  source = "${path.module}/../../assets/transform_jobs/${each.value.script}"
  etag   = filemd5("${path.module}/../../assets/transform_jobs/${each.value.script}")

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
