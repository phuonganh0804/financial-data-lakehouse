output "landing_bucket_name" {
  description = "Landing layer (raw, immutable) S3 bucket name"
  value       = aws_s3_bucket.landing.id
}

output "landing_bucket_arn" {
  description = "Landing layer (raw, immutable) S3 bucket ARN"
  value       = aws_s3_bucket.landing.arn
}

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

# Landing module
output "glue_landing_role_arn" {
  description = "Glue landing IAM role ARN"
  value       = module.landing_job.glue_role_arn
}

output "glue_landing_job_names" {
  description = "Created Glue landing job names"
  value       = module.landing_job.glue_job_names
}

# Bronze module
output "glue_bronze_role_arn" {
  description = "Glue bronze IAM role ARN"
  value       = module.bronze_job.glue_role_arn
}

output "glue_bronze_job_names" {
  description = "Created Glue bronze job names"
  value       = module.bronze_job.glue_job_names
}

# Transform module
output "glue_transform_role_arn" {
  description = "Glue transform IAM role ARN"
  value       = module.transform_job.glue_role_arn
}

output "glue_transform_job_names" {
  description = "Created Glue transform job names"
  value       = module.transform_job.glue_job_names
}

output "glue_catalog_database" {
  description = "Glue Catalog database name for Iceberg silver tables"
  value       = module.transform_job.glue_catalog_database
}

# Serving module
output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = module.serving.workgroup_name
}

output "athena_query_results" {
  description = "S3 location for Athena query results"
  value       = module.serving.query_results_location
}

output "gold_database" {
  description = "Glue Catalog database for the gold (dbt) serving layer"
  value       = module.serving.gold_database
}