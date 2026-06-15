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
      "s3:ListBucket"
    ]
    resources = [
      var.scripts_bucket_arn,
      "${var.scripts_bucket_arn}/*"
    ]
  }

  # Read-only on landing — bronze derives from the raw zone but must never
  # mutate it.
  statement {
    sid    = "ReadLandingBucket"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      var.landing_bucket_arn,
      "${var.landing_bucket_arn}/*"
    ]
  }

  statement {
    sid    = "ListBronzeBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
    ]
    resources = [
      var.bronze_bucket_arn,
    ]
  }

  statement {
    sid    = "WriteBronzeObjects"
    effect = "Allow"
    actions = [
      "s3:PutObject",
    ]
    resources = [
      "${var.bronze_bucket_arn}/*",
    ]
  }

  # Dynamic partition overwrite replaces only the partitions in a run, which
  # requires deleting the old objects in those partitions.
  statement {
    sid    = "DeleteOverwrittenBronzeData"
    effect = "Allow"
    actions = [
      "s3:DeleteObject",
    ]
    resources = [
      "${var.bronze_bucket_arn}/binance_klines/*",
      "${var.bronze_bucket_arn}/equity_prices/*",
      "${var.bronze_bucket_arn}/fred_macro/*",
    ]
  }

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

  statement {
    sid    = "DecryptKMS"
    effect = "Allow"
    actions = [
      "kms:Decrypt"
    ]
    resources = ["*"]
  }
}
