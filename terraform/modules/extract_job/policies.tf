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

  statement {
  sid    = "ListBronzeBucket"
  effect = "Allow"
  actions = [
    "s3:ListBucket"
  ]
  resources = [
    var.bronze_bucket_arn
  ]
}

statement {
  sid    = "WriteBronzeObjects"
  effect = "Allow"
  actions = [
    "s3:PutObject"
  ]
  resources = [
    "${var.bronze_bucket_arn}/*"
  ]
}

statement {
  sid    = "DeleteOverwrittenBronzeData"
  effect = "Allow"
  actions = [
    "s3:DeleteObject"
  ]
  resources = [
    "${var.bronze_bucket_arn}/fred_macro/*",
    "${var.bronze_bucket_arn}/binance_klines/*",
    "${var.bronze_bucket_arn}/equity_prices/*"
  ]
}

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
    sid    = "ReadSSMParameters"
    effect = "Allow"
    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters"
    ]
    resources = [
      "arn:aws:ssm:*:${data.aws_caller_identity.current.account_id}:parameter/financial-data-lakehouse/*"
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



