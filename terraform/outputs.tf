output "service_account_email" {
  description = "Email of the NYC Taxi service account"
  value       = google_service_account.nyc_taxi_sa.email
}

output "secret_ids" {
  description = "Map of secret names to their IDs"
  value = {
    minio_root_user         = google_secret_manager_secret.minio_root_user.secret_id
    minio_root_password     = google_secret_manager_secret.minio_root_password.secret_id
    minio_endpoint          = google_secret_manager_secret.minio_endpoint.secret_id
    minio_access_key        = google_secret_manager_secret.minio_access_key.secret_id
    minio_secret_key        = google_secret_manager_secret.minio_secret_key.secret_id
    postgres_user           = google_secret_manager_secret.postgres_user.secret_id
    postgres_password       = google_secret_manager_secret.postgres_password.secret_id
    postgres_db             = google_secret_manager_secret.postgres_db.secret_id
    postgres_url            = google_secret_manager_secret.postgres_url.secret_id
    airflow_admin_username  = google_secret_manager_secret.airflow_admin_username.secret_id
    airflow_admin_password  = google_secret_manager_secret.airflow_admin_password.secret_id
    airflow_admin_firstname = google_secret_manager_secret.airflow_admin_firstname.secret_id
    airflow_admin_lastname  = google_secret_manager_secret.airflow_admin_lastname.secret_id
    airflow_admin_email     = google_secret_manager_secret.airflow_admin_email.secret_id
    airflow_admin_role      = google_secret_manager_secret.airflow_admin_role.secret_id
  }
}

output "gcs_bucket_names" {
  description = "Names of created GCS buckets"
  value = {
    bronze  = google_storage_bucket.nyc_taxi_bronze.name
    silver  = google_storage_bucket.nyc_taxi_silver.name
    gold    = google_storage_bucket.nyc_taxi_gold.name
    airflow = google_storage_bucket.nyc_taxi_airflow.name
  }
}

output "gcs_bucket_urls" {
  description = "URLs of created GCS buckets"
  value = {
    bronze  = google_storage_bucket.nyc_taxi_bronze.url
    silver  = google_storage_bucket.nyc_taxi_silver.url
    gold    = google_storage_bucket.nyc_taxi_gold.url
    airflow = google_storage_bucket.nyc_taxi_airflow.url
  }
}

output "iam_roles_assigned" {
  description = "IAM roles assigned to the service account"
  value = [
    "roles/secretmanager.secretAccessor",
    "roles/storage.admin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser"
  ]
}
