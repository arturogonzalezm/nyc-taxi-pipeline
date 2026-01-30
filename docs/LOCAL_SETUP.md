# How to Run Locally

## Overview

This guide explains how to run the NYC Taxi Pipeline locally using Docker and Makefile commands.

## Prerequisites

| Requirement | Version | Purpose |
|-------------|---------|---------|
| Docker Desktop | Latest | Container runtime |
| Docker Compose | v2.0+ | Multi-container orchestration |
| Make | Any | Task automation |
| Git | Any | Clone repository |

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/arturogonzalezm/nyc-taxi-pipeline
cd nyc-taxi-pipeline
```

### 2. Start All Services

```bash
make up
```

This command:
- Creates `.env` file from template (if not exists)
- Starts MinIO (object storage)
- Starts PostgreSQL (data warehouse)
- Starts ETL container (PySpark)
- Displays service URLs and credentials

### 3. Verify Services

After `make up`, you should see:

```
Services started successfully!

Service URLs:
  MinIO API:      http://localhost:9000
  MinIO Console:  http://localhost:9001

MinIO Credentials:
  Username: minioadmin
  Password: minioadmin
```

## Docker Services

### Service Overview

| Service | Container Name | Ports | Purpose |
|---------|---------------|-------|---------|
| MinIO | nyc-taxi-minio | 9000, 9001 | S3-compatible object storage |
| MinIO Init | nyc-taxi-minio-init | - | Creates bucket on startup |
| PostgreSQL | nyc-taxi-postgres | 5432 | Data warehouse |
| ETL | nyc-taxi-etl | - | PySpark processing |

### Docker Compose Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Docker Compose                        │
├─────────────────────────────────────────────────────────┤
│  ┌─────────┐  ┌─────────────┐  ┌──────────────────────┐ │
│  │  MinIO  │  │  PostgreSQL │  │    ETL (PySpark)     │ │
│  │  :9000  │  │    :5432    │  │  Python 3.12 + Java  │ │
│  │  :9001  │  │             │  │                      │ │
│  └─────────┘  └─────────────┘  └──────────────────────┘ │
│       │              │                    │              │
│       └──────────────┴────────────────────┘              │
│                      │                                   │
│              Docker Network                              │
└─────────────────────────────────────────────────────────┘
```

## Makefile Commands

### General Commands

| Command | Description |
|---------|-------------|
| `make init` | Initialize project (create .env, directories) |
| `make up` | Start all services |
| `make down` | Stop all services |
| `make logs` | Show service logs |
| `make nuke` | Remove all containers, images, volumes |

### PostgreSQL Commands

| Command | Description |
|---------|-------------|
| `make postgres-start` | Start PostgreSQL only |
| `make postgres-stop` | Stop PostgreSQL |
| `make postgres-shell` | Connect to psql shell |
| `make postgres-status` | Show status and table counts |
| `make postgres-nuke` | Destroy and recreate PostgreSQL |

## Running ETL Jobs

### Option 1: Inside Docker Container

```bash
# Enter the ETL container
docker exec -it nyc-taxi-etl bash

# Run ingestion job
python -m etl.jobs.bronze.taxi_ingestion_job --taxi-type yellow --year 2024 --month 1

# Run transformation job
python -m etl.jobs.gold.taxi_gold_job --taxi-type yellow --year 2024 --month 1

# Run load job
python -m etl.jobs.load.postgres_load_job --taxi-type yellow --year 2024 --month 1
```

### Option 2: From Host (with local Python)

```bash
# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Run jobs (requires Java 17+)
python -m etl.jobs.bronze.taxi_ingestion_job --taxi-type yellow --year 2024 --month 1
```

## Environment Variables

The `.env` file contains configuration:

```bash
# MinIO Configuration
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=minio:9000

# PostgreSQL Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=nyc_taxi
```

## Accessing Services

### MinIO Console

1. Open http://localhost:9001
2. Login with `minioadmin` / `minioadmin`
3. Browse the `nyc-taxi-pipeline` bucket

### PostgreSQL

```bash
# Using make command
make postgres-shell

# Or directly with psql
psql -h localhost -p 5432 -U postgres -d nyc_taxi

# Sample queries
SELECT COUNT(*) FROM taxi.fact_trip;
SELECT * FROM taxi.dim_location LIMIT 10;
```

## Troubleshooting

### Services Won't Start

```bash
# Check Docker is running
docker info

# Check for port conflicts
lsof -i :9000
lsof -i :5432

# View detailed logs
docker-compose logs -f
```

### Clean Restart

```bash
# Stop and remove everything
make nuke

# Start fresh
make up
```

### Slow Image Downloads

If Docker images download slowly:

1. Check internet connection
2. Try a different network
3. Configure Docker registry mirror

### ETL Container Exits

The ETL container runs `tail -f /dev/null` to stay alive. If it exits:

```bash
# Check logs
docker logs nyc-taxi-etl

# Rebuild container
docker-compose build etl
docker-compose up -d etl
```

## Development Workflow

### Live Code Updates

The `./etl` folder is mounted as a volume, so code changes are reflected immediately:

```bash
# Edit code locally
vim etl/jobs/bronze/taxi_ingestion_job.py

# Run updated code in container
docker exec -it nyc-taxi-etl python -m etl.jobs.bronze.taxi_ingestion_job --help
```

### Running Tests

```bash
# From host (with local Python)
pytest tests/

# With coverage
pytest tests/ --cov=etl --cov-report=html
```

## Stopping Services

```bash
# Stop services (keep data)
make down

# Stop and remove all data
make nuke
```
