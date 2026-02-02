terraform {
  required_version = ">= 1.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable Secret Manager API
resource "google_project_service" "secretmanager" {
  project = var.project_id
  service = "secretmanager.googleapis.com"

  disable_on_destroy = false
}

# Service Account for accessing secrets
resource "google_service_account" "nyc_taxi_sa" {
  account_id   = "nyc-taxi-account"
  display_name = "NYC Taxi Pipeline Service Account"
  project      = var.project_id
}

# IAM binding for Secret Manager access
resource "google_project_iam_member" "secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.nyc_taxi_sa.email}"
}

# MinIO Secrets
resource "google_secret_manager_secret" "minio_root_user" {
  secret_id = "minio-root-user"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "minio_root_user" {
  secret      = google_secret_manager_secret.minio_root_user.id
  secret_data = var.minio_root_user
}

resource "google_secret_manager_secret" "minio_root_password" {
  secret_id = "minio-root-password"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "minio_root_password" {
  secret      = google_secret_manager_secret.minio_root_password.id
  secret_data = var.minio_root_password
}

resource "google_secret_manager_secret" "minio_endpoint" {
  secret_id = "minio-endpoint"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "minio_endpoint" {
  secret      = google_secret_manager_secret.minio_endpoint.id
  secret_data = var.minio_endpoint
}

resource "google_secret_manager_secret" "minio_access_key" {
  secret_id = "minio-access-key"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "minio_access_key" {
  secret      = google_secret_manager_secret.minio_access_key.id
  secret_data = var.minio_access_key
}

resource "google_secret_manager_secret" "minio_secret_key" {
  secret_id = "minio-secret-key"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "minio_secret_key" {
  secret      = google_secret_manager_secret.minio_secret_key.id
  secret_data = var.minio_secret_key
}

# PostgreSQL Secrets
resource "google_secret_manager_secret" "postgres_user" {
  secret_id = "postgres-user"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "postgres_user" {
  secret      = google_secret_manager_secret.postgres_user.id
  secret_data = var.postgres_user
}

resource "google_secret_manager_secret" "postgres_password" {
  secret_id = "postgres-password"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "postgres_password" {
  secret      = google_secret_manager_secret.postgres_password.id
  secret_data = var.postgres_password
}

resource "google_secret_manager_secret" "postgres_db" {
  secret_id = "postgres-db"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "postgres_db" {
  secret      = google_secret_manager_secret.postgres_db.id
  secret_data = var.postgres_db
}

resource "google_secret_manager_secret" "postgres_url" {
  secret_id = "postgres-url"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "postgres_url" {
  secret      = google_secret_manager_secret.postgres_url.id
  secret_data = var.postgres_url
}

# Airflow Secrets
resource "google_secret_manager_secret" "airflow_admin_username" {
  secret_id = "airflow-admin-username"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "airflow_admin_username" {
  secret      = google_secret_manager_secret.airflow_admin_username.id
  secret_data = var.airflow_admin_username
}

resource "google_secret_manager_secret" "airflow_admin_password" {
  secret_id = "airflow-admin-password"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "airflow_admin_password" {
  secret      = google_secret_manager_secret.airflow_admin_password.id
  secret_data = var.airflow_admin_password
}

resource "google_secret_manager_secret" "airflow_admin_firstname" {
  secret_id = "airflow-admin-firstname"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "airflow_admin_firstname" {
  secret      = google_secret_manager_secret.airflow_admin_firstname.id
  secret_data = var.airflow_admin_firstname
}

resource "google_secret_manager_secret" "airflow_admin_lastname" {
  secret_id = "airflow-admin-lastname"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "airflow_admin_lastname" {
  secret      = google_secret_manager_secret.airflow_admin_lastname.id
  secret_data = var.airflow_admin_lastname
}

resource "google_secret_manager_secret" "airflow_admin_email" {
  secret_id = "airflow-admin-email"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "airflow_admin_email" {
  secret      = google_secret_manager_secret.airflow_admin_email.id
  secret_data = var.airflow_admin_email
}

resource "google_secret_manager_secret" "airflow_admin_role" {
  secret_id = "airflow-admin-role"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "airflow_admin_role" {
  secret      = google_secret_manager_secret.airflow_admin_role.id
  secret_data = var.airflow_admin_role
}

# =============================================================================
# IAM ROLES
# =============================================================================

# Enable Cloud Storage API
resource "google_project_service" "storage" {
  project = var.project_id
  service = "storage.googleapis.com"

  disable_on_destroy = false
}

# IAM: Storage Admin for GCS bucket management
resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.nyc_taxi_sa.email}"
}

# IAM: BigQuery Data Editor for data operations
resource "google_project_iam_member" "bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.nyc_taxi_sa.email}"
}

# IAM: BigQuery Job User for running queries
resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.nyc_taxi_sa.email}"
}

# =============================================================================
# GOOGLE CLOUD STORAGE (GCS) BUCKETS
# =============================================================================

# GCS Bucket for raw/bronze data layer
resource "google_storage_bucket" "nyc_taxi_bronze" {
  name          = "${var.project_id}-nyc-taxi-bronze"
  location      = var.region
  project       = var.project_id
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    environment = "production"
    project     = "nyc-taxi-pipeline"
    layer       = "bronze"
  }

  depends_on = [google_project_service.storage]
}

# GCS Bucket for processed/silver data layer
resource "google_storage_bucket" "nyc_taxi_silver" {
  name          = "${var.project_id}-nyc-taxi-silver"
  location      = var.region
  project       = var.project_id
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 180
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    environment = "production"
    project     = "nyc-taxi-pipeline"
    layer       = "silver"
  }

  depends_on = [google_project_service.storage]
}

# GCS Bucket for analytics/gold data layer
resource "google_storage_bucket" "nyc_taxi_gold" {
  name          = "${var.project_id}-nyc-taxi-gold"
  location      = var.region
  project       = var.project_id
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = {
    environment = "production"
    project     = "nyc-taxi-pipeline"
    layer       = "gold"
  }

  depends_on = [google_project_service.storage]
}

# GCS Bucket for Airflow DAGs and logs
resource "google_storage_bucket" "nyc_taxi_airflow" {
  name          = "${var.project_id}-nyc-taxi-airflow"
  location      = var.region
  project       = var.project_id
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    environment = "production"
    project     = "nyc-taxi-pipeline"
    component   = "airflow"
  }

  depends_on = [google_project_service.storage]
}

# IAM binding for service account to access buckets
resource "google_storage_bucket_iam_member" "bronze_bucket_access" {
  bucket = google_storage_bucket.nyc_taxi_bronze.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.nyc_taxi_sa.email}"
}

resource "google_storage_bucket_iam_member" "silver_bucket_access" {
  bucket = google_storage_bucket.nyc_taxi_silver.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.nyc_taxi_sa.email}"
}

resource "google_storage_bucket_iam_member" "gold_bucket_access" {
  bucket = google_storage_bucket.nyc_taxi_gold.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.nyc_taxi_sa.email}"
}

resource "google_storage_bucket_iam_member" "airflow_bucket_access" {
  bucket = google_storage_bucket.nyc_taxi_airflow.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.nyc_taxi_sa.email}"
}
