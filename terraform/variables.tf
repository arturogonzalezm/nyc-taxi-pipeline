variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "nyc-taxi-pipeline"
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

# MinIO Variables
variable "minio_root_user" {
  description = "MinIO root user"
  type        = string
  sensitive   = true
}

variable "minio_root_password" {
  description = "MinIO root password"
  type        = string
  sensitive   = true
}

variable "minio_endpoint" {
  description = "MinIO endpoint"
  type        = string
  default     = "minio:9000"
}

variable "minio_access_key" {
  description = "MinIO access key"
  type        = string
  sensitive   = true
}

variable "minio_secret_key" {
  description = "MinIO secret key"
  type        = string
  sensitive   = true
}

# PostgreSQL Variables
variable "postgres_user" {
  description = "PostgreSQL user"
  type        = string
  sensitive   = true
}

variable "postgres_password" {
  description = "PostgreSQL password"
  type        = string
  sensitive   = true
}

variable "postgres_db" {
  description = "PostgreSQL database name"
  type        = string
  default     = "nyc_taxi"
}

variable "postgres_url" {
  description = "PostgreSQL JDBC URL"
  type        = string
  default     = "jdbc:postgresql://localhost:5432/nyc_taxi"
}

# Airflow Variables
variable "airflow_admin_username" {
  description = "Airflow admin username"
  type        = string
  default     = "admin"
}

variable "airflow_admin_password" {
  description = "Airflow admin password"
  type        = string
  sensitive   = true
}

variable "airflow_admin_firstname" {
  description = "Airflow admin first name"
  type        = string
  default     = "Admin"
}

variable "airflow_admin_lastname" {
  description = "Airflow admin last name"
  type        = string
  default     = "User"
}

variable "airflow_admin_email" {
  description = "Airflow admin email"
  type        = string
  default     = "admin@example.com"
}

variable "airflow_admin_role" {
  description = "Airflow admin role"
  type        = string
  default     = "Admin"
}
