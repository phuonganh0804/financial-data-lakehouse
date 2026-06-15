variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "bronze_bucket_name" {
  description = "Name of bronze S3 bucket"
  type        = string
}

variable "bronze_bucket_arn" {
  description = "ARN of bronze S3 bucket"
  type        = string
}

variable "silver_bucket_name" {
  description = "Name of silver S3 bucket"
  type        = string
}

variable "silver_bucket_arn" {
  description = "ARN of silver S3 bucket"
  type        = string
}

variable "scripts_bucket_name" {
  description = "Name of scripts S3 bucket"
  type        = string
}

variable "scripts_bucket_arn" {
  description = "ARN of scripts S3 bucket"
  type        = string
}

variable "catalog_database" {
  description = "Glue catalog database name for Iceberg tables"
  type        = string
}

variable "ingest_date" {
  description = "Bronze partition to read (YYYY-MM-DD)"
  type        = string
}

variable "interval" {
  description = "Data interval (1d, 1h, 15m)"
  type        = string
}

variable "transform_jobs" {
  description = "Map of transform jobs to create"
  type = map(object({
    script     = string
    table_name = string
    timeout    = number
  }))
}
