output "glue_role_arn" {
  description = "Shared Glue landing IAM role ARN"
  value       = aws_iam_role.glue_role.arn
}

output "glue_job_names" {
  description = "Map of created Glue landing job names"
  value       = { for k, v in aws_glue_job.landing_jobs : k => v.name }
}
