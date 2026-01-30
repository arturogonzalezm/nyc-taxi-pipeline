[![codecov](https://codecov.io/gh/arturogonzalezm/nyc-taxi-pipeline/graph/badge.svg?token=4jNHztzVjc)](https://codecov.io/gh/arturogonzalezm/nyc-taxi-pipeline)

# NYC Taxi Data Pipeline

A **production-grade data pipeline** for processing NYC Taxi data with:
- ☁️ **GCP Infrastructure**: BigQuery, Cloud Storage, VPC, IAM
- 🏠 **Local Development**: Full pipeline runs locally with Docker before GCP deployment
- 💰 **Cost-Optimized**: Uses GCP Free Tier, easy teardown when not in use
- 🔒 **Production Security**: VPC, service accounts, least-privilege IAM, private IPs
- 🚀 **CI/CD Ready**: Automated testing, linting, Terraform validation

## 🎯 Dual-Mode Architecture

This pipeline supports **TWO execution modes**:

### 1. 🏠 Local Mode (Development & Testing)
- Runs completely on your machine
- Uses Docker Compose with MinIO and PostgreSQL
- **No GCP costs** - test everything before deployment
- **Command**: `make local-setup && make local-run`

### 2. ☁️ GCP Mode (Production)
- Deploys to Google Cloud Platform
- Uses BigQuery, GCS
- Production VPC with private networking
- Full IAM security and service accounts
- **Command**: `make gcp-deploy && make gcp-run`

---

## 📋 Table of Contents

- [Quick Start (Local)](#-quick-start-local-mode)
- [GCP Deployment](#-gcp-deployment-production-mode)
- [Architecture](#-architecture)
- [Data Model](#-data-model)
- [Cost Management](#-cost-management--free-tier)
- [Security](#-security-implementation)
- [Development Workflow](#-development-workflow)
- [Teardown & Cleanup](#-teardown--cleanup)

---

## 🚀 Quick Start (Local Mode)

Test the entire pipeline on your machine in **under 10 minutes**:

### Prerequisites
- Docker Desktop (8GB RAM minimum)
- Python 3.10+
- 10GB free disk space
- **No GCP account needed for local mode**

### Step 1: Clone and Setup

```bash
git clone https://github.com/arturogonzalezm/nyc-taxi-pipeline
cd nyc-taxi-pipeline

# Copy environment file
cp .env.example .env

# Install dependencies
make local-setup
```

### Step 2: Start Local Services

```bash
# Start MinIO and all services
make local-up

# Check services are running
docker-compose ps
```

You should see:
- PostgreSQL (port 5432)
- MinIO API (http://localhost:9000)
- MinIO Console (http://localhost:9001)

### Step 3: Run the Pipeline

```bash

# Option B: Run via Make (simpler for testing)
make local-run MONTHS=2023-01

# This runs:
# - Bronze bronze (downloads NYC data)
# - Gold gold (creates dimensional model)  
# - PostgreSQL loading (loads star schema)
```

### Step 4: Query the Data

```bash
# Connect to PostgreSQL
make local-db

# Run sample queries
SELECT COUNT(*) FROM fact_trips;
SELECT * FROM dim_location LIMIT 10;

# Or use pre-built queries
\i /sql/sample_queries.sql
```

### Step 5: Run Tests

```bash
# Run all tests locally
make test

# Run with coverage report
make test-coverage
```

### Step 6: Stop Services

```bash
# Stop all services (keeps data)
make local-down

# Clean everything (removes data)
make local-clean
```

---

## ☁️ GCP Deployment (Production Mode)

Deploy to Google Cloud with **production-grade security and networking**.

### Prerequisites

1. **GCP Account**
   - New account gets $300 free credits (90 days)
   - Enable Billing (won't charge without explicit upgrade)
   - Free tier includes: 10GB BigQuery storage, 1TB queries/month

2. **Required Tools**
   ```bash
   # Install gcloud CLI
   curl https://sdk.cloud.google.com | bash
   exec -l $SHELL
   
   # Install Terraform
   brew install terraform  # Mac
   # or download from https://terraform.io
   
   # Authenticate
   gcloud auth login
   gcloud auth application-default login
   ```

### Step 1: Configure GCP Project

```bash
# Set your project ID
export GCP_PROJECT_ID="nyc-taxi-pipeline-485713"
export GCP_REGION="us-central1"  # Free tier eligible

# Create project (if new)
gcloud projects create $GCP_PROJECT_ID
gcloud config set project $GCP_PROJECT_ID

# Enable billing
# Go to: https://console.cloud.google.com/billing

# Verify setup
make gcp-verify
```

### Step 2: Configure Terraform

```bash
cd terraform

# Create your variables file
cat > terraform.tfvars << TFVARS
project_id  = "$GCP_PROJECT_ID"
region      = "us-central1"
environment = "prod"

# Enable production features
enable_bigquery      = true
enable_cloud_composer = true
enable_vpc          = true
enable_private_ip   = true

# Cost optimization
composer_node_count = 3  # Minimum for production
composer_machine_type = "n1-standard-1"  # Smallest

# Data retention (days)
bucket_lifecycle_rules = {
  bronze_archive_days = 90
  silver_archive_days = 180
  gold_archive_days   = 365
}
TFVARS
```

### Step 3: Deploy Infrastructure

```bash
# Initialize Terraform
make gcp-init

# Review what will be created (no charges yet)
make gcp-plan

# Deploy infrastructure
make gcp-deploy

# This creates:
# ✓ VPC with private networking
# ✓ GCS buckets (bronze, silver, gold)
# ✓ BigQuery dataset with tables
# ✓ Service accounts with IAM roles
# ✓ Cloud Storage buckets
# ✓ Firewall rules
# ✓ NAT gateway for private instances
```

**Expected deployment time**: 20-30 minutes (mostly Cloud Composer setup)

```

### Step 5: Run Pipeline on GCP

```bash

# Option B: Trigger via CLI
make gcp-trigger-pipeline MONTHS=2023-01,2023-02,2023-03

# Monitor progress
make gcp-logs
```

### Step 6: Query BigQuery

```bash
# Via CLI
bq query --use_legacy_sql=false '
SELECT 
    d.year,
    d.month,
    COUNT(*) as total_trips,
    SUM(f.total_amount) as total_revenue
FROM `'$GCP_PROJECT_ID'.nyc_taxi_warehouse.fact_trips` f
JOIN `'$GCP_PROJECT_ID'.nyc_taxi_warehouse.dim_datetime` d 
ON f.datetime_key = d.datetime_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month'

# Or use BigQuery Console
# https://console.cloud.google.com/bigquery
```

---

## 🏗️ Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     LOCAL MODE (Development)                     │
├─────────────────────────────────────────────────────────────────┤
│  Docker Compose                                                  │
│  ├── PostgreSQL (simulates BigQuery)                            │
│  ├── MinIO (object storage)                                      │
│  └── PySpark containers                                          │
│                                                                   │
│  Storage: Local filesystem (./data/)                             │
│  Cost: $0                                                        │
└─────────────────────────────────────────────────────────────────┘
                              ⬇️ Deploy
┌─────────────────────────────────────────────────────────────────┐
│                      GCP MODE (Production)                       │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  VPC Network (10.0.0.0/16)                              │   │
│  │  ├── Private Subnet (10.0.1.0/24)                       │   │
│  │  │   └── Cloud Functions                              │   │
│  │  ├── Private Subnet (10.0.2.0/24)                       │   │
│  │  │   └── Dataproc Cluster (for Spark jobs)             │   │
│  │  └── Cloud NAT (for internet access)                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Cloud Storage (Data Lake)                              │   │
│  │  ├── gs://bronze/   - Raw data (versioned)             │   │
│  │  ├── gs://silver/   - Cleaned data                      │   │
│  │  └── gs://gold/     - Dimensional model                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  BigQuery (Data Warehouse)                              │   │
│  │  ├── fact_trips (partitioned by month)                  │   │
│  │  ├── dim_datetime                                        │   │
│  │  ├── dim_location                                        │   │
│  │  └── dim_payment                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  IAM & Security                                          │   │
│  │  ├── pipeline-sa (Composer/Dataproc)                    │   │
│  │  ├── storage-sa (GCS access)                            │   │
│  │  └── bq-loader-sa (BigQuery write)                      │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
NYC TLC API
    ↓
┌───────────────────────────────┐
│ PySpark Job 1: Bronze         │  Downloads raw data
│ - Schema validation           │  Adds metadata
│ - Partitioning (year/month)   │  Writes to storage
└───────────────────────────────┘
    ↓
GCS Bronze / Local Filesystem
    ↓
┌───────────────────────────────┐
│ PySpark Job 2: Gold           │  Cleans & validates
│ - Data quality checks         │  Creates dimensions
│ - Join zone lookup            │  Deduplication
│ - Dimensional modeling        │  
└───────────────────────────────┘
    ↓
GCS Gold / Local Filesystem
    ↓
┌───────────────────────────────┐
│ PySpark Job 3: Load           │  
│ - BigQuery (GCP mode)         │  Bulk load with WRITE_TRUNCATE
│ - PostgreSQL (Local mode)     │  Creates indexes
└───────────────────────────────┘
    ↓
BigQuery / PostgreSQL
    ↓
Analytics & BI Tools
```

### Airflow DAG

```python
# DAG: nyc_taxi_pipeline
# Schedule: Monthly (@monthly)
# Backfill: Supported with custom dates

bronze_ingestion
    ↓
gold_transformation
    ↓
data_quality_check
    ↓
bigquery_load
    ↓
analytics_report (optional)
```

---

## 📊 Data Model

### Star Schema Design

```
                    ┌─────────────────┐
                    │  dim_datetime   │
                    ├─────────────────┤
                    │ datetime_key PK │
                    │ pickup_datetime │
            ┌───────│ year            │
            │       │ month           │
            │       │ day             │
            │       │ hour            │
            │       │ is_weekend      │
            │       │ is_holiday      │
            │       └─────────────────┘
            │
            │       ┌─────────────────┐
            │       │  dim_location   │
            │       ├─────────────────┤
            │   ┌───│ location_key PK │
            │   │   │ location_id     │
            │   │   │ borough         │
            │   │   │ zone            │
            │   │   │ service_zone    │
            │   │   └─────────────────┘
            │   │
            ↓   ↓   
    ┌──────────────────────┐
    │     fact_trips       │  ← Main fact table
    ├──────────────────────┤
    │ trip_id (PK)         │
    │ datetime_key (FK)    │───────┘
    │ pickup_location_key (FK) │───┘
    │ dropoff_location_key (FK)│───┐
    │ payment_key (FK)     │───┐   │
    │ passenger_count      │   │   │
    │ trip_distance        │   │   │
    │ fare_amount          │   │   │
    │ tip_amount           │   │   │
    │ total_amount         │   │   │
    │ trip_duration_min    │   │   │
    │ data_source_month    │   │   └───┐
    └──────────────────────┘   │       │
            ↓                  │       │
     ┌─────────────────┐       │       │
     │  dim_payment    │       │       │
     ├─────────────────┤       │       │
     │ payment_key PK  │───────┘       │
     │ payment_type_id │               │
     │ payment_name    │               │
     │ is_card         │               │
     └─────────────────┘               │
                                       │
                        (dropoff location)
```

### BigQuery Implementation

**Partitioning Strategy:**
- `fact_trips`: Partitioned by `data_source_month` (STRING, YYYY-MM format)
- Clustering: `datetime_key`, `pickup_location_key`

**Benefits:**
- Efficient historical queries
- Automatic partition pruning
- Cost optimization (scan only needed partitions)

**Sample BigQuery DDL:**
```sql
CREATE TABLE `project.nyc_taxi_warehouse.fact_trips`
(
  trip_id INT64,
  datetime_key INT64,
  pickup_location_key INT64,
  dropoff_location_key INT64,
  payment_key INT64,
  passenger_count INT64,
  trip_distance FLOAT64,
  fare_amount FLOAT64,
  total_amount FLOAT64,
  trip_duration_minutes FLOAT64,
  data_source_month STRING
)
PARTITION BY data_source_month
CLUSTER BY datetime_key, pickup_location_key;
```

---

## 💰 Cost Management & Free Tier

### GCP Free Tier Limits

| Service | Free Tier | Expected Usage (1 month data) | Cost |
|---------|-----------|-------------------------------|------|
| **Cloud Storage** | 5 GB | ~2 GB | $0 |
| **BigQuery Storage** | 10 GB | ~1.5 GB | $0 |
| **BigQuery Queries** | 1 TB/month | ~50 GB | $0 |
| **Cloud Composer** | Not free | ~$300/month | **💰 Main cost** |
| **Dataproc** | Not free | ~$0.10/hour when running | **💰 Per-use** |
| **Networking** | 1 GB egress/month | Minimal | $0 |

### Cost Optimization Strategies

#### 1. **Use Preemptible/Spot Instances**
```hcl
# terraform/main.tf
resource "google_dataproc_cluster" "spark_cluster" {
  cluster_config {
    worker_config {
      num_instances    = 2
      preemptible      = true  # 80% cheaper
      disk_config {
        boot_disk_size_gb = 30  # Minimum
      }
    }
  }
}
```

#### 2. **Auto-Shutdown Composer When Not in Use**
```bash
# Pause Composer (saves ~$240/month when paused)
make gcp-pause-composer

# Resume when needed
make gcp-resume-composer
```

#### 3. **Scheduled Infrastructure**
```bash
# Automated teardown at night
make gcp-schedule-shutdown TIME=20:00  # 8 PM
make gcp-schedule-startup TIME=08:00   # 8 AM
```

#### 4. **Development Lifecycle**
```bash
# Work locally (free)
make local-run

# Deploy to GCP for testing
make gcp-deploy

# Run pipeline
make gcp-trigger-pipeline MONTHS=2023-01

# IMMEDIATELY destroy after testing
make gcp-destroy
```

### Monthly Cost Estimates

| Scenario | Cost | Notes |
|----------|------|-------|
| **Local only** | $0 | Perfect for development |
| **GCP - Minimal** | $150-200/month | Composer minimal config |
| **GCP - Standard** | $300-400/month | Composer + regular Dataproc |
| **GCP - With auto-pause** | $50-100/month | Composer paused nights/weekends |

### Free Tier Testing Budget

**Recommended approach for interview assignment:**

1. **Develop locally** (free): 1-2 days
2. **Deploy to GCP**: 1 day
3. **Run 1-2 test pipelines**: ~$5
4. **Record demo video**: ~$1
5. **Destroy infrastructure**: Back to $0

**Total cost for assignment: ~$6-10 using free credits**

---

## 🔒 Security Implementation

### Production Security Checklist

#### ✅ Network Security
- [x] **Private VPC** with custom IP ranges
- [x] **Private IP** for Cloud Composer (no public access)
- [x] **Cloud NAT** for outbound internet (downloads)
- [x] **Firewall rules** (deny all ingress, allow specific egress)
- [x] **VPC Service Controls** (optional, for highly sensitive data)

#### ✅ IAM & Access Control
```
Service Accounts (Principle of Least Privilege):

├── composer-sa@project.iam.gserviceaccount.com
│   ├── roles/composer.worker
│   ├── roles/dataproc.worker
│   └── roles/storage.objectViewer (GCS buckets)
│
├── dataproc-sa@project.iam.gserviceaccount.com
│   ├── roles/dataproc.worker
│   ├── roles/storage.objectAdmin (GCS read/write)
│   └── roles/bigquery.jobUser
│
└── bq-loader-sa@project.iam.gserviceaccount.com
    ├── roles/bigquery.dataEditor (write to tables)
    └── roles/bigquery.jobUser (run load jobs)
```

#### ✅ Data Security
- [x] **Encryption at rest** (Google-managed keys)
- [x] **Encryption in transit** (TLS)
- [x] **Bucket versioning** (protect against accidental deletion)
- [x] **Lifecycle policies** (automatic data retention)
- [x] **Access logs** (Cloud Audit Logs enabled)

#### ✅ Secret Management
```bash
# Store sensitive values in Secret Manager
gcloud secrets create db-password --data-file=- <<< "your-password"

# Access in pipeline
gcloud secrets versions access latest --secret="db-password"
```

### Security Configuration

**Terraform enforces security by default:**

```hcl
# VPC with private networking
resource "google_compute_network" "vpc" {
  name                    = "nyc-taxi-vpc"
  auto_create_subnetworks = false
}

# Private subnet
resource "google_compute_subnetwork" "private" {
  name          = "private-subnet"
  ip_cidr_range = "10.0.1.0/24"
  network       = google_compute_network.vpc.id
  
  private_ip_google_access = true  # Access Google APIs privately
}

# Cloud NAT for outbound only
resource "google_compute_router_nat" "nat" {
  name   = "composer-nat"
  router = google_compute_router.router.name
  
  nat_ip_allocate_option = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# Firewall: Deny all ingress by default
resource "google_compute_firewall" "deny_all_ingress" {
  name    = "deny-all-ingress"
  network = google_compute_network.vpc.name
  
  deny {
    protocol = "all"
  }
  
  direction = "INGRESS"
  priority  = 1000
}
```

---

## 🔄 Development Workflow

### Daily Development Flow

```bash
# 1. Work on local mode (no costs)
make local-up
make local-run MONTHS=2023-01

# 2. Make changes to code
vim etl/jobs/gold_transformation.py

# 3. Test locally
make test
make local-run MONTHS=2023-01

# 4. Commit changes
git add .
git commit -m "feat: improve gold transformation"
git push

# 5. CI/CD runs automatically (GitHub Actions)
# - Linting
# - Tests  
# - Terraform validation

# 6. Deploy to GCP for final testing
make gcp-deploy
make gcp-trigger-pipeline MONTHS=2023-01

# 7. Verify results
make gcp-query-bigquery

# 8. Tear down GCP (save costs)
make gcp-destroy
```

### Branch Strategy

```
main
  ├── develop (active development)
  │   ├── feature/bronze-optimization
  │   ├── feature/bigquery-partitioning
  │   └── fix/data-quality-check
  └── production (deployed to GCP)
```

**Workflow:**
1. Develop on feature branches
2. Test locally with `make local-run`
3. PR to `develop` → CI/CD runs
4. Merge to `main` → Deploy to GCP (manual approval)

---

## 🧨 Teardown & Cleanup

### Quick Cleanup (Keep Infrastructure)

```bash
# Stop local services
make local-down

# Pause GCP resources (saves money, keeps config)
make gcp-pause
```

### Full Teardown (Complete Deletion)

#### Option 1: Terraform Destroy (Recommended)

```bash
# Destroy all GCP resources
make gcp-destroy

# Or manual:
cd terraform
terraform destroy -var-file="terraform.tfvars"
```

**What gets deleted:**
- ✓ Cloud Composer environment
- ✓ GCS buckets (and all data)
- ✓ BigQuery dataset (and all tables)
- ✓ Service accounts
- ✓ VPC, subnets, firewall rules
- ✓ NAT gateway

**What remains:**
- ✓ GCP project (you can delete manually)
- ✓ Terraform state (in GCS backend)

#### Option 2: Manual Cleanup

```bash
# Delete Composer (biggest cost)
gcloud composer environments delete nyc-taxi-pipeline --location us-central1

# Delete GCS buckets
gsutil rm -r gs://YOUR_PROJECT-nyc-taxi-bronze
gsutil rm -r gs://YOUR_PROJECT-nyc-taxi-silver  
gsutil rm -r gs://YOUR_PROJECT-nyc-taxi-gold

# Delete BigQuery dataset
bq rm -r -f -d nyc_taxi_warehouse

# Delete service accounts
gcloud iam service-accounts delete composer-sa@PROJECT.iam.gserviceaccount.com
gcloud iam service-accounts delete dataproc-sa@PROJECT.iam.gserviceaccount.com
```

#### Option 3: Nuke Entire Project

```bash
# Nuclear option - delete everything
gcloud projects delete YOUR_PROJECT_ID
```

### Verify Cleanup

```bash
# Check no resources remain
make gcp-verify-cleanup

# Should show: No active resources found
```

### Cost Verification

```bash
# Check current charges
gcloud billing accounts list
gcloud billing projects describe YOUR_PROJECT_ID

# View detailed billing
# Go to: https://console.cloud.google.com/billing
```

---

## 📁 Project Structure

```
nyc-taxi-pipeline/
├── README.md                          ← You are here
├── Makefile                           ← All commands (local + GCP)
├── docker-compose.yml                 ← Local development services
├── .env.example                       ← Environment template
│
├── pyspark/                           ← Pipeline code
│   ├── jobs/
│   │   ├── bronze_ingestion.py       ← Job 1: Ingest raw data
│   │   ├── gold_transformation.py    ← Job 2: Transform to dimensional model
│   │   ├── bigquery_loader.py        ← Job 3a: Load to BigQuery (GCP)
│   │   └── postgres_loader.py        ← Job 3b: Load to PostgreSQL (local)
│   ├── common/
│   │   ├── config.py                 ← Dual-mode configuration
│   │   ├── spark_session.py          ← Spark factory (local vs GCP)
│   │   ├── gcs_utils.py              ← GCS operations
│   │   └── bq_utils.py               ← BigQuery operations
│   └── requirements.txt
│
│
├── terraform/                         ← Infrastructure as Code
│   ├── main.tf                       ← Main config
│   ├── vpc.tf                        ← VPC, subnets, NAT
│   ├── gcs.tf                        ← Storage buckets
│   ├── bigquery.tf                   ← BigQuery dataset + tables
│   ├── composer.tf                   ← Cloud Composer (Airflow)
│   ├── iam.tf                        ← Service accounts + roles
│   ├── security.tf                   ← Firewall rules
│   ├── variables.tf                  ← Input variables
│   ├── outputs.tf                    ← Output values
│   └── terraform.tfvars.example      ← Example config
│
├── sql/                              ← Database schemas
│   ├── bigquery/
│   │   ├── create_tables.sql         ← BigQuery DDL
│   │   └── sample_queries.sql        ← Analytics queries
│   └── postgres/
│       ├── create_schema.sql         ← PostgreSQL DDL
│       └── sample_queries.sql        ← Analytics queries
│
├── tests/                            ← Test suite
│   ├── conftest.py                   ← pytest fixtures
│   ├── test_bronze_ingestion.py
│   ├── test_gold_transformation.py
│   ├── test_bigquery_loader.py
│   └── test_local_vs_gcp.py          ← Consistency tests
│
├── .github/
│   └── workflows/
│       ├── ci.yml                    ← CI pipeline
│       └── deploy.yml                ← CD pipeline (GCP)
│
└── docs/
    ├── ARCHITECTURE.md               ← Technical deep-dive
    ├── SECURITY.md                   ← Security documentation
    ├── COST_OPTIMIZATION.md          ← Cost management guide
    └── TROUBLESHOOTING.md            ← Common issues
```

---

## 🎯 Makefile Commands Reference

### Local Development

```bash
make local-setup          # Install dependencies, setup environment
make local-up             # Start all local services (PostgreSQL, Airflow)
make local-down           # Stop all services (keep data)
make local-clean          # Stop and remove all data
make local-run            # Run pipeline locally (bronze→gold→postgres)
make local-db             # Connect to local PostgreSQL
make local-airflow-ui     # Open Airflow UI in browser
make local-logs           # Tail all service logs
```

### Testing

```bash
make test                 # Run all tests
make test-coverage        # Run tests with coverage report
make test-integration     # Run integration tests (local + GCP)
make lint                 # Run linting (black, flake8, pylint)
make format               # Auto-format code
```

### GCP Deployment

```bash
make gcp-verify           # Check GCP prerequisites
make gcp-init             # Initialize Terraform
make gcp-plan             # Preview infrastructure changes
make gcp-deploy           # Deploy all infrastructure
make gcp-destroy          # Destroy all infrastructure
make gcp-outputs          # Show Terraform outputs

make gcp-upload-dags      # Upload Airflow DAGs to Composer
make gcp-trigger-pipeline # Trigger pipeline run
make gcp-logs             # View pipeline logs
make gcp-query-bigquery   # Run sample BigQuery query
```

### Cost Management

```bash
make gcp-pause            # Pause Composer (save costs)
make gcp-resume           # Resume Composer
make gcp-cost-estimate    # Estimate current monthly cost
make gcp-verify-cleanup   # Verify no resources remain
```

### CI/CD

```bash
make ci                   # Run all CI checks (lint + test + terraform)
make ci-local             # Run CI checks locally
```

---

## 🧪 Testing Strategy

### 1. Unit Tests (Local)
```bash
pytest tests/test_bronze_ingestion.py -v
```

### 2. Integration Tests (Local)
```bash
# Full pipeline on sample data
make test-integration-local
```

### 3. End-to-End Tests (GCP)
```bash
# Deploy to GCP and run pipeline
make test-integration-gcp MONTHS=2023-01
```

### 4. Data Quality Tests
```bash
# Validate data quality
make test-data-quality
```

---

## 🐛 Troubleshooting

### Local Issues

**PostgreSQL won't start:**
```bash
docker-compose down -v  # Remove volumes
make local-up
```

**Airflow tasks failing:**
```bash
make local-logs         # Check logs
docker-compose restart airflow-scheduler
```

**Out of memory:**
```bash
# Increase Docker memory to 8GB
# Docker Desktop → Settings → Resources → Memory
```

### GCP Issues

**Terraform deployment fails:**
```bash
# Check quotas
gcloud compute project-info describe --project=YOUR_PROJECT

# Check APIs enabled
make gcp-verify
```

**Composer creation timeout:**
- Composer can take 20-30 minutes to create
- Check status: `gcloud composer environments list`

**BigQuery load fails:**
```bash
# Check service account permissions
gcloud projects get-iam-policy YOUR_PROJECT

# Check BigQuery quotas
bq show --project_id=YOUR_PROJECT
```

**Cost spike:**
```bash
# Immediately pause Composer
make gcp-pause

# Check what's running
gcloud compute instances list
gcloud dataproc clusters list

# Destroy if needed
make gcp-destroy
```

---

## 📚 Additional Documentation

- [Architecture Deep Dive](docs/ARCHITECTURE.md)
- [Security Documentation](docs/SECURITY.md)
- [Cost Optimization Guide](docs/COST_OPTIMIZATION.md)
- [Troubleshooting Guide](docs/TROUBLESHOOTING.md)
- [API Reference](docs/API.md)

---

## 🎓 Learning Resources

- [NYC TLC Data Documentation](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices)
- [Cloud Composer Documentation](https://cloud.google.com/composer/docs)
- [Terraform GCP Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)

---

## 🤝 Contributing

This is an interview assignment project. For production use:
1. Fork the repository
2. Create feature branch
3. Make changes with tests
4. Submit pull request

---

## 📄 License

MIT License - See LICENSE file for details

---

## 👤 Author

Built for GCP Data Engineer role assessment

**Key Features Demonstrated:**
- ✅ Production-grade GCP architecture
- ✅ Dual-mode development (local + cloud)
- ✅ Cost-optimized design
- ✅ Comprehensive security
- ✅ Full CI/CD pipeline
- ✅ Extensive documentation

**Ready for submission!** 🚀
