# Terraform - GCP Infrastructure Setup

This Terraform configuration manages GCP infrastructure for the NYC Taxi Pipeline project:
- **Secret Manager**: Stores sensitive credentials for PostgreSQL, MinIO, and Airflow
- **IAM Roles**: Configures service account permissions (Secret Manager, Storage, BigQuery)
- **GCS Buckets**: Creates data lake storage (bronze, silver, gold layers) and Airflow storage

## Prerequisites

1. [Terraform](https://www.terraform.io/downloads) >= 1.14.4
2. [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
3. GCP Project: `nyc-taxi-pipeline-485713`

## Setup

### 1. Authenticate with GCP

```bash
gcloud auth application-default login
gcloud config set project nyc-taxi-pipeline-485713
```

### 2. Initialize Terraform

```bash
cd terraform
terraform init
```

### 3. Create terraform.tfvars

Copy the example file and fill in your secret values:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your actual secret values.

### 4. Apply the Configuration

```bash
terraform plan
terraform apply
```

## Resources Created

- **Service Account**: `nyc-taxi-account` - Used by the application to access secrets
- **IAM Binding**: Grants `roles/secretmanager.secretAccessor` to the service account
- **Secrets**: All configuration values from `.env` are stored in Secret Manager:
  - MinIO: `minio-root-user`, `minio-root-password`, `minio-endpoint`, `minio-access-key`, `minio-secret-key`
  - PostgreSQL: `postgres-user`, `postgres-password`, `postgres-db`, `postgres-url`
  - Airflow: `airflow-admin-username`, `airflow-admin-password`, `airflow-admin-firstname`, `airflow-admin-lastname`, `airflow-admin-email`, `airflow-admin-role`

## Accessing Secrets

### Using gcloud CLI

```bash
gcloud secrets versions access latest --secret="postgres-password"
```

### Using Python

```python
from google.cloud import secretmanager

client = secretmanager.SecretManagerServiceClient()
name = "projects/nyc-taxi-pipeline-485713/secrets/postgres-password/versions/latest"
response = client.access_secret_version(request={"name": name})
secret_value = response.payload.data.decode("UTF-8")
```

## Clean Up

To destroy all resources:

```bash
terraform destroy
```
