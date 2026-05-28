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

  # Scripts bucket - read only
  statement {
    sid    = "ReadScriptsBucket"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      var.scripts_bucket_arn,
      "${var.scripts_bucket_arn}/*"
    ]
  }

  # Bronze bucket - write only
  statement {
    sid    = "WriteBronzeBucket"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:ListBucket"
    ]
    resources = [
      var.bronze_bucket_arn,
      "${var.bronze_bucket_arn}/*"
    ]
  }

  # Glue catalog - needed for Iceberg
  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetPartitions",
      "glue:BatchCreatePartition"
    ]
    resources = ["*"]
  }

  # CloudWatch - logging only
  statement {
    sid    = "CloudWatchLogging"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:*:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
}

