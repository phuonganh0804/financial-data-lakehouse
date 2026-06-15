data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

resource "aws_s3_bucket" "landing" {
  bucket = format("%s-landing-%s-%s-an",
    var.project_name,
    data.aws_caller_identity.current.account_id,
    data.aws_region.current.region
  )

  bucket_namespace = "account-regional"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "landing"
  }
}

resource "aws_s3_bucket" "bronze" {
  bucket = format("%s-bronze-%s-%s-an",
    var.project_name,
    data.aws_caller_identity.current.account_id,
    data.aws_region.current.region
  )

  bucket_namespace = "account-regional"


  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "bronze"
  }
}

resource "aws_s3_bucket" "silver" {
  bucket = format("%s-silver-%s-%s-an",
    var.project_name,
    data.aws_caller_identity.current.account_id,
    data.aws_region.current.region
  )

  bucket_namespace = "account-regional"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "silver"
  }
}

resource "aws_s3_bucket" "gold" {
  bucket = format("%s-gold-%s-%s-an",
    var.project_name,
    data.aws_caller_identity.current.account_id,
    data.aws_region.current.region
  )

  bucket_namespace = "account-regional"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "gold"
  }
}

resource "aws_s3_bucket" "scripts" {
  bucket = format("%s-scripts-%s-%s-an",
    var.project_name,
    data.aws_caller_identity.current.account_id,
    data.aws_region.current.region
  )

  bucket_namespace = "account-regional"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "scripts"
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "landing" {
  bucket                  = aws_s3_bucket.landing.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket                  = aws_s3_bucket.scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Versioning on scripts bucket only
resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Versioning on landing — the raw zone is immutable and append-only;
# versioning guards the byte-for-byte API payloads against accidental
# overwrite or deletion so reprocessing is always possible.
resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id
  versioning_configuration {
    status = "Enabled"
  }
}