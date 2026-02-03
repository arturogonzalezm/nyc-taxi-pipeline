variable "project_id" {
  description = "Base name for the GCP Project ID"
  type        = string
  default     = "nyc-taxi-pipeline"
}

variable "instance_number" {
  description = "Instance number (01, 02, etc.)"
  type        = string
  default     = "01"
}

variable "project_name" {
  description = "GCP Project display name"
  type        = string
  default     = "NYC Taxi Pipeline"
}

variable "billing_account_id" {
  description = "GCP Billing Account ID"
  type        = string
  sensitive   = true
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
  description = "Environment name (dev, prod)"
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

# Construct the full project ID
locals {
  project_id = "${var.project_id}-${var.environment}-${var.instance_number}"
}
