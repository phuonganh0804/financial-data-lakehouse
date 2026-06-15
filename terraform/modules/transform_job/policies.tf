data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "glue_base_policy" {
  statement {
    sid    = "AllowGlueToAssumeRole"
    effect = "Allow"
    principals {
      identifiers = ["glue.amazonaws.com"]
      type        = "Service"
    }
    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "glue_access_policy" {
  statement {
    sid    = "ReadScriptsBucket"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      var.scripts_bucket_arn,
      "${var.scripts_bucket_arn}/*",
    ]
  }

  statement {
    sid    = "ReadBronzeBucket"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      var.bronze_bucket_arn,
      "${var.bronze_bucket_arn}/*",
    ]
  }

  statement {
    sid    = "ReadWriteSilverBucket"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      var.silver_bucket_arn,
      "${var.silver_bucket_arn}/*",
    ]
  }

  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
    ]
    resources = [
      "arn:aws:glue:*:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:*:${data.aws_caller_identity.current.account_id}:database/default",
      "arn:aws:glue:*:${data.aws_caller_identity.current.account_id}:database/${var.catalog_database}",
      "arn:aws:glue:*:${data.aws_caller_identity.current.account_id}:table/${var.catalog_database}/*",
    ]
  }

  # Glue Data Quality — evaluate the rulesets against the silver tables and
  # publish results/metrics. Used by standalone DQ runs and the Airflow DQ task
  # (both assume this role). cloudwatch:PutMetricData is for DQ score metrics.
  statement {
    sid    = "GlueDataQuality"
    effect = "Allow"
    # Wildcard covers the full DQ run lifecycle (start/get/publish results +
    # statistic annotations) so we don't chase one missing action at a time.
    # Tighten to an explicit least-privilege list later if desired.
    actions = [
      "glue:*DataQuality*",
      "cloudwatch:PutMetricData",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "CloudWatchLogging"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:*:${data.aws_caller_identity.current.account_id}:*",
    ]
  }

  statement {
    sid    = "DecryptKMS"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
    ]
    resources = ["*"]
  }
}
