output "glue_role_arn" {
  description = "Shared Glue bronze IAM role ARN"
  value       = aws_iam_role.glue_role.arn
}

output "glue_job_names" {
  description = "Map of created Glue bronze job names"
  value       = { for k, v in aws_glue_job.bronze_jobs : k => v.name }
}
