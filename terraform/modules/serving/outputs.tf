output "workgroup_name" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.main.name
}

output "query_results_location" {
  description = "S3 location for Athena query results"
  value       = "s3://${var.gold_bucket_name}/athena-results/"
}

output "gold_database" {
  description = "Glue Catalog database for the gold (dbt) serving layer"
  value       = aws_glue_catalog_database.gold.name
}
