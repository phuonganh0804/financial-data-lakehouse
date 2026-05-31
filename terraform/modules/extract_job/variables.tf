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
  default     = "2026-05-31"
}

variable "api_start_date" {
  description = "API start date (YYYY-MM-DD)"
  type        = string
}

variable "api_end_date" {
  description = "API end date (YYYY-MM-DD)"
  type        = string
}

variable "interval" {
  description = "Data interval (1d, 1h, 15m)"
  type        = string
}

variable "ticker_config_path" {
  description = "S3 URI to DAX ticker JSON config"
  type        = string
}

variable "extract_jobs" {
  description = "Map of extract jobs to create"
  type = map(object({
    script  = string
    timeout = number
  }))
}