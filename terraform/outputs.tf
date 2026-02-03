output "project_id" {
  description = "The GCP project ID"
  value       = google_project.nyc_taxi_project.project_id
}

output "project_number" {
  description = "The GCP project number"
  value       = google_project.nyc_taxi_project.number
}

output "service_account_email" {
  description = "Email of the NYC Taxi service account"
  value       = google_service_account.nyc_taxi_sa.email
}

output "gcs_bucket_name" {
  description = "Name of the GCS bucket"
  value       = google_storage_bucket.nyc_taxi_pipeline.name
}

output "gcs_bucket_url" {
  description = "URL of the GCS bucket"
  value       = google_storage_bucket.nyc_taxi_pipeline.url
}

output "iam_roles_assigned" {
  description = "IAM roles assigned to the service account"
  value = [
    "roles/storage.admin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser"
  ]
}

# Workload Identity Federation outputs for GitHub Actions
output "workload_identity_provider" {
  description = "Workload Identity Provider resource name (use this for GCP_WORKLOAD_IDENTITY_PROVIDER secret)"
  value       = google_iam_workload_identity_pool_provider.github_provider.name
}

output "workload_identity_pool" {
  description = "Workload Identity Pool resource name"
  value       = google_iam_workload_identity_pool.github_pool.name
}
