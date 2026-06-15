variable "project_name" {
  type        = string
  description = "Project name"
}

variable "environment" {
  type        = string
  description = "Environment name"
}

variable "gold_bucket_name" {
  type        = string
  description = "Name of the gold S3 bucket for Athena query results"
}

variable "gold_database" {
  type        = string
  description = "Glue Catalog database name for the gold (dbt) serving layer"
}
