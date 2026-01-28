# NYC Taxi Mini Pipeline

## Overview

Design and implement a data pipeline using Apache Spark to process NYC Taxi data, load it into a database dimensional model, and provision core infrastructure using Terraform on GCP.

You are free to choose the architecture, modeling strategy, orchestration approach, and CI/CD tooling.

You may execute the pipeline on **Docker locally** but infrastructure definitions must target GCP (even if not deployed).

## Dataset (Public)

Use NYC TLC Yellow Taxi trip data and Taxi Zone Lookup.

### Yellow Taxi Trip Data (Parquet)

Example month:
```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
```

General pattern:
```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
```

### Taxi Zone Lookup (CSV)

```
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

You may process 1-3 months of data depending on your design and time constraints.

## Objectives

Design and implement a functioning data pipeline that accomplishes the following:

### 1. PySpark Job 1: Bronze Ingestion

Implement a PySpark job that ingests raw data into the Bronze layer:

- Download the selected Yellow Taxi trip dataset(s)
- Download taxi zone lookup
- Store them into GCS bronze zone (or local folder if using Docker):
  ```
  gs://<bucket>/bronze/nyc_taxi/<your-structure>/
  ```
- Must support historical data ingestion (e.g., ability to pass a list of months or a date range)
- Preserve raw data with minimal transformation (schema enforcement, add metadata columns if needed)

You may structure the bronze layout however you prefer - document your logic.

### 2. PySpark Job 2: Dimensional Model Transformation

Implement a second PySpark job that reads from Bronze and creates the dimensional model:

- Reads bronze layer taxi trip data
- Cleans, filters, and normalizes fields
- Joins zone lookup metadata
- Creates a dimensional model with fact and dimension tables
- Writes output into a curated/gold zone in your chosen format (parquet/orc/etc.)

You design the dimensional model:

- Define fact table(s) with appropriate grain
- Define dimension table(s) as needed
- Choose partitioning strategy
- Document your modeling decisions

### 3. Build a Database Dimensional Model

Load your curated output into any database engine you choose (running locally in Docker).

Options include:

- PostgreSQL
- BigQuery (bonus)

Document your model with relations.

### 4. Terraform GCP Infrastructure Definition

Create a Terraform module that defines at minimum:

- GCS bucket(s)
- Service account(s)
- Any roles needed for your pipeline

You may design more infrastructure (Cloud Composer, Cloud Run, BigQuery datasets, networks), but only GCS + IAM are required.

### 5. Historical Data Logic

Demonstrate how your pipeline:

- Processes multiple historical months
- Can re-run or backfill a specific month
- Avoids duplicates or data corruption

A simple strategy is acceptable - document your logic.

### 6. CI/CD (Required)

Provide either:

- A GitHub Actions workflow
- GitLab CI
- A simple Makefile + script-based CI simulation

CI/CD must include at least:

- Linting and/or formatting
- Running Spark job tests (unit tests)
- Validating Terraform (terraform fmt + terraform validate)

Bonus if you include:

- Docker image build
- Deploy pipeline container
- Execute sample pipeline run

### 7. Optional: Orchestrator

Include an orchestration tool if time permits (e.g., Airflow, Dagster, Prefect).

## Deliverables

### Code

- PySpark Job 1: Bronze ingestion
- PySpark Job 2: Dimensional model transformation
- DB schema creation / loading logic
- CI/CD configuration
- Terraform modules
- Unit tests (PyTest or similar)

### Documentation

README.md describing:

1. Architecture diagram
2. Dataset explanation
3. Data model and schema (fact + dims)
4. Historical strategy
5. How to run locally (Docker)
6. How Terraform is structured
7. Limitations and what you would improve next
