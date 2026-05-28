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

variable "scripts_bucket_name" {
  description = "Name of scripts S3 bucket"
  type        = string
}

variable "scripts_bucket_arn" {
  description = "ARN of scripts S3 bucket"
  type        = string
}

variable "ingest_date" {
  description = "Date to ingest data for"
  type        = string
  default     = "2024-01-01"
}

variable "extract_jobs" {
  description = "Map of extract jobs to create"
  type = map(object({
    script  = string
    timeout = number
  }))
}