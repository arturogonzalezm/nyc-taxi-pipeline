# =============================================================================
# GCP PROJECT
# =============================================================================

# Enable Cloud Resource Manager API (required for project operations)
resource "google_project_service" "cloudresourcemanager" {
  project = var.project_id
  service = "cloudresourcemanager.googleapis.com"

  disable_on_destroy = false
}

# Enable Cloud Billing API (required before creating projects with billing)
resource "google_project_service" "cloudbilling" {
  project = var.project_id
  service = "cloudbilling.googleapis.com"

  disable_on_destroy = false

  depends_on = [google_project_service.cloudresourcemanager]
}

# Create the GCP Project
resource "google_project" "nyc_taxi_project" {
  name            = var.project_name
  project_id      = var.project_id
  billing_account = var.billing_account_id

  depends_on = [google_project_service.cloudbilling]

  labels = {
    environment = var.environment
    project     = "nyc-taxi-pipeline"
  }
}

# Enable IAM Service Account Credentials API (required for Workload Identity Federation)
resource "google_project_service" "iamcredentials" {
  project = var.project_id
  service = "iamcredentials.googleapis.com"

  disable_on_destroy = false

  depends_on = [google_project.nyc_taxi_project]
}

# Enable Service Usage API (required for listing/managing project services)
resource "google_project_service" "serviceusage" {
  project = var.project_id
  service = "serviceusage.googleapis.com"

  disable_on_destroy = false

  depends_on = [google_project.nyc_taxi_project]
}

# Service Account for the pipeline
resource "google_service_account" "nyc_taxi_sa" {
  account_id   = "nyc-taxi-account"
  display_name = "NYC Taxi Pipeline Service Account"
  project      = var.project_id
}

# =============================================================================
# WORKLOAD IDENTITY FEDERATION (for GitHub Actions)
# =============================================================================

# Enable IAM API
resource "google_project_service" "iam" {
  project = var.project_id
  service = "iam.googleapis.com"

  disable_on_destroy = false

  depends_on = [google_project.nyc_taxi_project]
}

# Workload Identity Pool for GitHub Actions
resource "google_iam_workload_identity_pool" "github_pool" {
  project                   = var.project_id
  workload_identity_pool_id = "github-actions-pool"
  display_name              = "GitHub Actions Pool"
  description               = "Identity pool for GitHub Actions CI/CD"

  depends_on = [google_project_service.iam]
}

# Workload Identity Provider for GitHub OIDC
resource "google_iam_workload_identity_pool_provider" "github_provider" {
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub Provider"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }

  attribute_condition = "assertion.repository == '${var.github_repository}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# Allow GitHub Actions to impersonate the service account
resource "google_service_account_iam_member" "github_actions_impersonation" {
  service_account_id = google_service_account.nyc_taxi_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_pool.name}/attribute.repository/${var.github_repository}"
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

# Single GCS Bucket for NYC Taxi Pipeline
resource "google_storage_bucket" "nyc_taxi_pipeline" {
  name          = "nyc-taxi-${var.environment}-etl-${var.region}-${var.bucket_suffix}"
  location      = var.region
  project       = var.project_id
  force_destroy = true

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
    environment = var.environment
    project     = "nyc-taxi-pipeline"
  }

  depends_on = [google_project_service.storage]
}

# IAM binding for service account to access bucket
resource "google_storage_bucket_iam_member" "pipeline_bucket_access" {
  bucket = google_storage_bucket.nyc_taxi_pipeline.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.nyc_taxi_sa.email}"
}
