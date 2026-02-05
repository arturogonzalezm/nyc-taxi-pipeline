# Terraform Infrastructure for NYC Taxi Pipeline

This directory contains Terraform configuration for provisioning Google Cloud Platform (GCP) infrastructure for the NYC Taxi Pipeline project.

---

## Terraform Structure

### File Organization

| File | Purpose | Contents |
|------|---------|----------|
| `main.tf` | Primary resource definitions | GCP project, service account, GCS bucket, Workload Identity Federation, IAM bindings |
| `variables.tf` | Input variable declarations | All configurable parameters with types, defaults, and descriptions |
| `outputs.tf` | Output value definitions | Values exported for use by other systems (CI/CD, documentation) |
| `providers.tf` | Provider configuration | Google Cloud provider version constraints and authentication settings |
| `terraform.tfvars` | Variable values | Environment-specific configuration (not committed to version control) |

### Resource Grouping in main.tf

The `main.tf` file is organized into logical sections:

```hcl
# =============================================================================
# GCP PROJECT
# =============================================================================
# - google_project.nyc_taxi_project
# - google_project_service.* (API enablement)
# - google_service_account.nyc_taxi_sa

# =============================================================================
# WORKLOAD IDENTITY FEDERATION (for GitHub Actions)
# =============================================================================
# - google_iam_workload_identity_pool.github_pool
# - google_iam_workload_identity_pool_provider.github_provider
# - google_service_account_iam_member.github_actions_impersonation

# =============================================================================
# IAM ROLES
# =============================================================================
# - google_project_iam_member.storage_admin
# - google_project_iam_member.bigquery_data_editor
# - google_project_iam_member.bigquery_job_user

# =============================================================================
# GOOGLE CLOUD STORAGE (GCS) BUCKETS
# =============================================================================
# - google_storage_bucket.nyc_taxi_pipeline
# - google_storage_bucket_iam_member.pipeline_bucket_access
```

---

## Architecture and Design

### Design Principles

1. **Infrastructure as Code (IaC)**: All resources are defined declaratively in Terraform, enabling version control, code review, and reproducible deployments.

2. **Least Privilege Access**: Service accounts are granted only the minimum permissions required for pipeline operations.

3. **Environment Isolation**: The naming convention supports multiple environments (dev, staging, prod) with isolated resources.

4. **Secure CI/CD Integration**: Workload Identity Federation eliminates the need for long-lived service account keys in GitHub Actions.

### Resource Dependencies

```
┌─────────────────────────────────────────────────────────────────┐
│                        GCP Project                              │
│         (${project_id_base}-${environment}-${region}-${instance_number})    │
└─────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
          ▼                   ▼                   ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ Service Account │  │   GCS Bucket    │  │ Workload Identity│
│ (nyc-taxi-acct) │  │ (data storage)  │  │   Federation     │
└─────────────────┘  └─────────────────┘  └─────────────────┘
          │                   │                   │
          │                   │                   │
          ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                         IAM Bindings                            │
│  - Storage Admin          - BigQuery Data Editor                │
│  - BigQuery Job User      - Workload Identity User              │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **GitHub Actions** authenticates via Workload Identity Federation
2. **Terraform** provisions and manages infrastructure
3. **ETL Pipeline** uses the service account to read/write data to GCS
4. **BigQuery** (future) will consume data from GCS for analytics

---

## State Management

Terraform state is currently stored locally. For production environments, consider:

- **Remote State**: Store state in a GCS bucket with versioning enabled
- **State Locking**: Use Cloud Storage object locking to prevent concurrent modifications
- **State Encryption**: Enable default encryption on the state bucket

---

## Resources Created

| Resource Type | Name/Identifier | Description |
|---------------|-----------------|-------------|
| GCP Project | `${project_id_base}-${environment}-${region}-${instance_number}` | Primary project for all resources |
| Service Account | `${project_id_base}-${environment}-sa-${instance_number}@<project_id>.iam.gserviceaccount.com` | Service account for pipeline operations |
| GCS Bucket | `${project_id_base}-${environment}-gcs-${region}-${bucket_suffix}` | Data lake storage bucket |
| Workload Identity Pool | `github-actions-pool` | Identity pool for GitHub Actions |
| Workload Identity Provider | `github-provider` | OIDC provider for GitHub authentication |

### IAM Roles Assigned

The service account is granted the following roles:

- `roles/storage.admin` - Full control of GCS resources
- `roles/bigquery.dataEditor` - Read/write access to BigQuery datasets
- `roles/bigquery.jobUser` - Permission to run BigQuery jobs

---

## Configuration Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `project_id` | string | `${project_id_base}-${environment}-${region}-${instance_number}` | GCP Project ID |
| `project_name` | string | `NYC Taxi Pipeline` | GCP Project display name |
| `billing_account_id` | string | - | GCP Billing Account ID (required) |
| `region` | string | `us-central1` | GCP Region |
| `zone` | string | `us-central1-a` | GCP Zone |
| `environment` | string | `dev` | Environment name (dev, staging, prod) |
| `bucket_suffix` | string | `01` | Bucket suffix number |
| `github_repository` | string | - | GitHub repository for Workload Identity Federation (required) |

---

## Outputs

| Output | Description |
|--------|-------------|
| `project_id` | The GCP project ID |
| `project_number` | The GCP project number |
| `service_account_email` | Email of the NYC Taxi service account |
| `gcs_bucket_name` | Name of the GCS bucket |
| `gcs_bucket_url` | URL of the GCS bucket |
| `workload_identity_provider` | Workload Identity Provider resource name |
| `workload_identity_pool` | Workload Identity Pool resource name |

---

## Prerequisites

1. [Terraform](https://www.terraform.io/downloads) version 1.5.0 or higher
2. [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) (gcloud CLI)
3. GCP Billing Account with appropriate permissions
4. GitHub repository for CI/CD integration

### Required GCP Permissions (Bootstrap)

Before running Terraform for the first time, the authenticated user/service account needs the following permissions at the **organization or folder level**:

| Permission | Role | Purpose |
|------------|------|----------|
| `resourcemanager.projects.create` | `roles/resourcemanager.projectCreator` | Create new GCP projects |
| `billing.resourceAssociations.create` | `roles/billing.user` | Link projects to billing account |
| `iam.workloadIdentityPools.create` | `roles/iam.workloadIdentityPoolAdmin` | Create Workload Identity Federation pools |
| `storage.buckets.create` | `roles/storage.admin` | Create GCS buckets |

**Quick Setup (for project owners):**

```bash
# Grant yourself the required roles (replace with your email and org/folder ID)
ORG_ID="your-org-id"  # or use --folder=FOLDER_ID
USER_EMAIL="your-email@example.com"

# Project Creator (to create new projects)
gcloud organizations add-iam-policy-binding $ORG_ID \
  --member="user:$USER_EMAIL" \
  --role="roles/resourcemanager.projectCreator"

# Billing User (to link billing accounts)
gcloud organizations add-iam-policy-binding $ORG_ID \
  --member="user:$USER_EMAIL" \
  --role="roles/billing.user"
```

**Note:** If you're deploying to an **existing project** (not creating a new one), you need `roles/owner` or the following roles on that project:
- `roles/iam.workloadIdentityPoolAdmin`
- `roles/storage.admin`
- `roles/iam.serviceAccountAdmin`
- `roles/serviceusage.serviceUsageAdmin`

---

## Deployment

### Automated Deployment (GitHub Actions)

Infrastructure is automatically deployed via GitHub Actions when changes are merged to the `main` branch.

Required GitHub Secrets:

| Secret Name | Description |
|-------------|-------------|
| `GCP_CREDENTIALS` | Service account JSON key |
| `GCP_PROJECT_ID` | GCP project identifier |
| `GCP_BILLING_ACCOUNT_ID` | Billing account identifier |

### Manual Deployment

```bash
# Authenticate with GCP
gcloud auth application-default login
gcloud config set project <YOUR_PROJECT_ID>

# Initialize Terraform
cd terraform
terraform init

# Review the execution plan
terraform plan

# Apply the configuration
terraform apply
```

---

## Clean Up

### Via GitHub Actions

Trigger the `destroy.yml` workflow manually from the Actions tab.

### Via Command Line

```bash
cd terraform
terraform destroy
```

**Warning**: This will permanently delete all resources including the GCS bucket and its contents.
