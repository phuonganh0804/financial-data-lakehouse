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


output "bronze_bucket_arn" {
  description = "Bronze layer S3 bucket ARN"
  value       = aws_s3_bucket.bronze.arn
}

output "scripts_bucket_arn" {
  description = "Scripts S3 bucket ARN"
  value       = aws_s3_bucket.scripts.arn
}

output "silver_bucket_arn" {
  description = "Silver layer S3 bucket ARN"
  value       = aws_s3_bucket.silver.arn
}

output "gold_bucket_arn" {
  description = "Gold layer S3 bucket ARN"
  value       = aws_s3_bucket.gold.arn
}

output "glue_extract_role_arn" {
  description = "Glue extract IAM role ARN"
  value       = module.extract_job.glue_role_arn
}

output "glue_extract_role_name" {
  description = "Glue extract IAM role name"
  value       = module.extract_job.glue_role_name
}

output "glue_job_names" {
  description = "Created Glue extract job names"
  value       = module.extract_job.glue_job_names
}