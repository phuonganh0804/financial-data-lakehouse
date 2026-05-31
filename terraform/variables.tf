variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-central-1"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "dax-crypto-pipeline"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "ingest_date" {
  description = "Date this ingestion run represents (YYYY-MM-DD)"
  type        = string
}

variable "api_start_date" {
  description = "Start date for API data fetch, inclusive (YYYY-MM-DD)"
  type        = string
}

variable "api_end_date" {
  description = "End date for API data fetch, exclusive (YYYY-MM-DD)"
  type        = string
}

variable "interval" {
  description = "Data interval (1d, 1h, 15m)"
  type        = string
}