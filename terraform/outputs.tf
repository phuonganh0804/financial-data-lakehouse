output "bronze_bucket_name" {
  description = "Bronze layer S3 bucket name"
  value       = aws_s3_bucket.bronze.id
}

output "silver_bucket_name" {
  description = "Silver layer S3 bucket name"
  value       = aws_s3_bucket.silver.id
}

output "gold_bucket_name" {
  description = "Gold layer S3 bucket name"
  value       = aws_s3_bucket.gold.id
}

output "scripts_bucket_name" {
  description = "Scripts S3 bucket name"
  value       = aws_s3_bucket.scripts.id
}

output "account_id" {
  description = "AWS Account ID"
  value       = data.aws_caller_identity.current.account_id
}

output "aws_region" {
  description = "AWS Region"
  value       = data.aws_region.current.region
}