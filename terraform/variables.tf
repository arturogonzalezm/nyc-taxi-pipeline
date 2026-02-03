variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "nyc-taxi-pipeline-001"
}

variable "project_name" {
  description = "GCP Project display name"
  type        = string
  default     = "NYC Taxi Pipeline"
}

variable "billing_account_id" {
  description = "GCP Billing Account ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP Zone"
  type        = string
  default     = "us-central1-a"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "bucket_suffix" {
  description = "Bucket suffix number (e.g., 01, 02)"
  type        = string
  default     = "01"
}

# GitHub Variables
variable "github_repository" {
  description = "GitHub repository in format 'owner/repo' for Workload Identity Federation"
  type        = string
}
