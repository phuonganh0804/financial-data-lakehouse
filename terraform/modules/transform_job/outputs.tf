output "glue_role_arn" {
  description = "Glue transform IAM role ARN"
  value       = aws_iam_role.glue_role.arn
}

output "glue_job_names" {
  description = "Map of created Glue transform job names"
  value       = { for k, v in aws_glue_job.transform_jobs : k => v.name }
}

output "glue_catalog_database" {
  description = "Glue Catalog database name for Iceberg silver tables"
  value       = aws_glue_catalog_database.transform_db.name
}
