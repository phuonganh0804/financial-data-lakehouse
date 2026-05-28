output "glue_role_arn" {
  description = "Shared Glue extract IAM role ARN"
  value       = aws_iam_role.glue_role.arn
}

output "glue_role_name" {
  description = "Shared Glue extract IAM role name"
  value       = aws_iam_role.glue_role.name
}

output "glue_job_names" {
  description = "Map of created Glue job names"
  value       = { for k, v in aws_glue_job.extract_jobs : k => v.name }
}


