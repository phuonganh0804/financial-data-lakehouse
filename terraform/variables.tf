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