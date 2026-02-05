[![Unit Tests](https://github.com/arturogonzalezm/nyc-taxi-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/arturogonzalezm/nyc-taxi-pipeline/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/arturogonzalezm/nyc-taxi-pipeline/graph/badge.svg?token=4jNHztzVjc)](https://codecov.io/gh/arturogonzalezm/nyc-taxi-pipeline)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Linting: flake8](https://img.shields.io/badge/linting-flake8-yellowgreen.svg)](https://flake8.pycqa.org/)
[![Linting: pylint](https://img.shields.io/badge/linting-pylint-blue.svg)](https://pylint.pycqa.org/)

# NYC Taxi Data Pipeline

A production-ready PySpark ETL pipeline for processing NYC Taxi & Limousine Commission (TLC) trip data. The pipeline implements a medallion architecture (Bronze/Gold layers) with a dimensional model, supporting both Google Cloud Storage (GCS) and MinIO for data lake storage, with PostgreSQL for analytics.

## Table of Contents

- [Architecture](#architecture)
- [Infrastructure](#infrastructure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Running the Pipeline](#running-the-pipeline)
- [Testing](#testing)
- [Documentation](#documentation)

### Additional Documentation

- [Architecture Details](docs/ARCHITECTURE.md)
- [Dataset Explanation](docs/DATASET.md)
- [Data Model and Schema](docs/DATA_MODEL.md)
- [Historical Strategy](docs/HISTORICAL_STRATEGY.md)
- [Local Setup Guide](docs/LOCAL_SETUP.md)
- [Terraform Infrastructure](terraform/README.md)

## Architecture

The pipeline follows a medallion architecture with three layers:

```mermaid
sequenceDiagram
    participant NYC as NYC TLC API
    participant Bronze as Bronze Layer
    participant Gold as Gold Layer
    participant PG as PostgreSQL

    NYC->>Bronze: Download parquet files
    Note over Bronze: Raw data + metadata<br/>(record_hash, timestamps)
    Bronze->>Gold: Transform & validate
    Note over Gold: Dimensional model<br/>(dim_date, dim_location, etc.)
    Gold->>PG: Load star schema
    Note over PG: fact_trip + dimensions<br/>(idempotent upsert)
```

### Layer Overview

| Layer | Purpose | Storage |
|-------|---------|---------|
| Bronze | Raw data ingestion with metadata columns | GCS or MinIO |
| Gold | Dimensional model with data quality checks | GCS or MinIO |
| Load | Star schema for analytics | PostgreSQL |

### Storage Backends

The pipeline supports two storage backends:

| Backend | Use Case | Configuration |
|---------|----------|---------------|
| **Google Cloud Storage (GCS)** | Production deployments on GCP | `STORAGE_BACKEND=gcs` |
| **MinIO** | Local development and testing | `STORAGE_BACKEND=minio` |

## Infrastructure

The project includes Terraform configuration for automated GCP infrastructure provisioning:

- **GCP Project** with billing configuration
- **Service Account** for pipeline operations
- **GCS Bucket** for data lake storage (pattern: `${project_id_base}-${environment}-gcs-${region}-${bucket_suffix}`)
- **Workload Identity Federation** for secure GitHub Actions authentication
- **IAM Roles** for storage and BigQuery access

See [terraform/README.md](terraform/README.md) for detailed infrastructure documentation.

### CI/CD Workflows

| Workflow | Trigger | Purpose |
|----------|---------|----------|
| `ci.yml` | PR to main/develop | Linting, tests, security scan |
| `deploy.yml` | Push to main | Deploy Terraform infrastructure |
| `destroy.yml` | Manual | Destroy infrastructure |

---

## Prerequisites

- Python 3.12+
- Docker Desktop (for MinIO and PostgreSQL in local development)
- Java 17+ (for PySpark)
- Apache Spark 4.1.1
- Google Cloud SDK (for GCS deployments)
- Terraform 1.5.0

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/arturogonzalezm/nyc-taxi-pipeline
cd nyc-taxi-pipeline

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install ".[dev]"

# Initialize environment
make init
```

### 2. Start Services

```bash
# Start MinIO and PostgreSQL
make up
```

Services available:

[//]: # (- MinIO API: http://localhost:9000)

[//]: # (- MinIO Console: http://localhost:9001)
- PostgreSQL: localhost:5432

### 3. Run the Pipeline

```bash
# Ingest a single month of yellow taxi data
python -m etl.jobs.bronze.taxi_ingestion_job --taxi-type yellow --year 2024 --month 1

# Transform to dimensional model
python -m etl.jobs.gold.taxi_gold_job --taxi-type yellow --year 2024 --month 1

# Load to PostgreSQL
python -m etl.jobs.load.postgres_load_job --taxi-type yellow --year 2024 --month 1
```

### 4. Query the Data

```bash
# Connect to PostgreSQL
make postgres-shell

# Sample queries
SELECT COUNT(*) FROM taxi.fact_trip;
SELECT * FROM taxi.dim_location LIMIT 10;
```

### 5. Stop Services

```bash
make down
```

## Running the Pipeline

### Bronze Layer (Ingestion)

Downloads raw parquet files from NYC TLC and stores them in MinIO with metadata columns.

```bash
# Single month
python -m etl.jobs.bronze.taxi_ingestion_job \
    --taxi-type yellow \
    --year 2024 \
    --month 1

# Bulk ingestion (date range)
python -m etl.jobs.bronze.taxi_ingestion_job \
    --taxi-type yellow \
    --start-year 2023 --start-month 1 \
    --end-year 2023 --end-month 12

# Green taxi data
python -m etl.jobs.bronze.taxi_ingestion_job \
    --taxi-type green \
    --year 2024 \
    --month 1
```

### Zone Lookup (Reference Data)

Ingests the taxi zone lookup CSV for location dimension.

```bash
python -m etl.jobs.bronze.zone_lookup_ingestion_job
```

### Gold Layer (Transformation)

Transforms bronze data into a dimensional model with data quality checks.

```bash
# Single month
python -m etl.jobs.gold.taxi_gold_job \
    --taxi-type yellow \
    --year 2024 \
    --month 1

# Date range
python -m etl.jobs.gold.taxi_gold_job \
    --taxi-type yellow \
    --year 2023 --month 1 \
    --end-year 2023 --end-month 6
```

### Load Layer (PostgreSQL)

Loads the dimensional model into PostgreSQL using idempotent upserts.

```bash
# Load all data for a taxi type
python -m etl.jobs.load.postgres_load_job --taxi-type yellow

# Load specific month
python -m etl.jobs.load.postgres_load_job \
    --taxi-type yellow \
    --year 2024 \
    --month 1
```

### Safe Backfill

For re-processing historical data without duplicates:

```bash
# Backfill specific months
python etl/jobs/bronze/taxi_injection_safe_backfill_job.py yellow 2023-03 2023-07

# Backfill a range
python etl/jobs/bronze/taxi_injection_safe_backfill_job.py yellow 2023-01:2023-12

# Skip deletion (keep existing data)
python etl/jobs/bronze/taxi_injection_safe_backfill_job.py yellow 2024-01 --no-delete
```

## Makefile Commands

### General

| Command | Description |
|---------|-------------|
| `make init` | Initialize directories and create `.env` from template |
| `make up` | Start all services (MinIO + PostgreSQL) |
| `make down` | Stop all services |
| `make logs` | Show service logs |
| `make nuke` | Remove all containers, images, and volumes |

### PostgreSQL

| Command | Description |
|---------|-------------|
| `make postgres-start` | Start PostgreSQL container |
| `make postgres-stop` | Stop PostgreSQL container |
| `make postgres-shell` | Connect to PostgreSQL shell |
| `make postgres-status` | Show PostgreSQL status and table counts |
| `make postgres-create-tables` | Create dimensional model tables |
| `make postgres-nuke` | Destroy and recreate PostgreSQL with fresh schema |

### Help

```bash
make help
```

## Project Structure

```
nyc-taxi-pipeline/
├── etl/
│   ├── jobs/
│   │   ├── base_job.py              # Abstract base class (Template Method pattern)
│   │   ├── bronze/
│   │   │   ├── taxi_ingestion_job.py           # NYC taxi data ingestion
│   │   │   ├── taxi_injection_safe_backfill_job.py  # Safe historical backfill
│   │   │   └── zone_lookup_ingestion_job.py    # Zone lookup reference data
│   │   ├── gold/
│   │   │   └── taxi_gold_job.py     # Dimensional model transformation
│   │   ├── load/
│   │   │   └── postgres_load_job.py # PostgreSQL loader
│   │   └── utils/
│   │       ├── config.py            # Configuration (Singleton pattern)
│   │       └── spark_manager.py     # Spark session management
│   └── __init__.py
├── terraform/                       # GCP infrastructure (Terraform)
├── tests/                           # Unit tests (548+ tests, 68%+ coverage)
├── sql/
│   └── postgres/
│       └── create_dimensional_model.sql  # PostgreSQL schema DDL
├── docs/                            # Additional documentation
├── docker-compose.yml               # MinIO and PostgreSQL services
├── Makefile                         # Build automation
├── pyproject.toml                   # Project configuration and dependencies
└── README.md
```

## Data Model

### Star Schema

The gold layer produces a star schema with the following tables:

![alt text](https://github.com/arturogonzalezm/nyc-taxi-pipeline/blob/main/docs/images/erd.png?raw=true)

### Idempotent Loading

The PostgreSQL loader uses hash-based upserts to ensure idempotency:
- Each fact record has a `fact_hash` computed from business keys
- Re-running the load job skips existing records
- Safe to run multiple times without creating duplicates

## Testing

The project includes comprehensive unit tests with 548+ tests and 68%+ code coverage.

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=etl --cov-report=term

# Run specific test file
pytest tests/test_base_job.py -v
```

### Code Quality

```bash
# Format code
black etl/ tests/

# Lint
flake8 etl/ --max-line-length=100 --ignore=E501,W503
pylint etl/
```

## Environment Variables

Configure via environment variables or a secrets manager.

### Storage Configuration

| Variable | Description | Default |
|----------|-------------|----------|
| `STORAGE_BACKEND` | Storage backend (`gcs` or `minio`) | `minio` |
| `GCS_BUCKET` | GCS bucket name | Set via `.env` (pattern: `${project_id_base}-${environment}-gcs-${region}-${bucket_suffix}`) |
| `GCP_PROJECT_ID` | GCP project ID | Set via `.env` (pattern: `${project_id_base}-${environment}-${region}-${instance_number}`) |
| `MINIO_ENDPOINT` | MinIO server endpoint | `localhost:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO secret key | `minioadmin` |
| `MINIO_BUCKET` | MinIO bucket name | `nyc-taxi-pipeline` |

### Database Configuration

| Variable | Description |
|----------|-------------|
| `POSTGRES_HOST` | PostgreSQL host |
| `POSTGRES_PORT` | PostgreSQL port |
| `POSTGRES_DB` | PostgreSQL database |
| `POSTGRES_USER` | PostgreSQL user |
| `POSTGRES_PASSWORD` | PostgreSQL password |

> **Note:** Never commit credentials to version control. Use environment variables, secrets managers, or CI/CD secrets for production deployments.

---

## License

This project is for demonstration purposes.
