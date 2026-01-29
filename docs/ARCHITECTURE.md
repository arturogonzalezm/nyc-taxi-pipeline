# NYC Taxi Pipeline - Architecture Documentation

## Overview

This document provides a detailed technical architecture for the NYC Taxi Data Pipeline, a production-grade data engineering solution that supports both local development and GCP deployment.

## System Architecture

### High-Level Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                              │
│  - NYC TLC Trip Record Data (Parquet files)                 │
│  - NYC Taxi Zone Lookup (CSV)                               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                 Ingestion Layer (Bronze)                     │
│  - PySpark jobs download and validate raw data              │
│  - Minimal transformations (add metadata)                   │
│  - Store in partitioned format (year/month)                 │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              Transformation Layer (Gold)                     │
│  - Data quality checks and cleaning                         │
│  - Dimensional modeling (star schema)                       │
│  - Create fact and dimension tables                         │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   Storage Layer                              │
│  Local: PostgreSQL    |    GCP: BigQuery                    │
│  - Fact tables        |    - Partitioned by month           │
│  - Dimension tables   |    - Clustered by keys              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              Analytics & Visualization                       │
│  - SQL queries, Jupyter notebooks, BI tools                 │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow

### Bronze Layer Processing

1. **Download**: Fetch parquet files from NYC TLC CDN
2. **Validate**: Check schema and required fields
3. **Enrich**: Add metadata (ingestion timestamp, source info)
4. **Partition**: Store by year/month for efficient querying
5. **Persist**: Write to GCS (cloud) or local filesystem

### Gold Layer Processing

1. **Read**: Load bronze data for specified months
2. **Clean**: Apply data quality filters
   - Remove null timestamps
   - Filter unrealistic values (distance, fare, duration)
   - Handle outliers
3. **Transform**: Create dimensional model
   - Generate datetime dimension
   - Generate location dimension
   - Generate payment dimension
   - Create fact table with foreign keys
4. **Persist**: Write star schema to gold layer

### Loading Layer

1. **Read**: Load gold dimensional model
2. **Load**: Bulk insert to target database
   - PostgreSQL (local mode): JDBC bulk insert
   - BigQuery (GCP mode): Load from GCS
3. **Index**: Create indexes for query performance
4. **Verify**: Check row counts and data integrity

## Technology Stack

### Core Technologies

- **PySpark 3.5.0**: Distributed data processing
- **Apache Airflow 2.7.3**: Workflow orchestration
- **PostgreSQL 15**: Local data warehouse
- **Docker Compose**: Local development environment

### GCP Services

- **Cloud Storage**: Data lake (bronze/silver/gold)
- **BigQuery**: Data warehouse
- **Cloud Composer**: Managed Airflow
- **Dataproc**: Managed Spark clusters
- **VPC**: Private networking
- **IAM**: Service accounts and permissions

### Infrastructure as Code

- **Terraform 1.5+**: GCP resource provisioning
- **GitHub Actions**: CI/CD pipelines

## Data Model

### Star Schema Design

**Fact Table: fact_trips**
- trip_id (PK)
- datetime_key (FK → dim_datetime)
- pickup_location_key (FK → dim_location)
- dropoff_location_key (FK → dim_location)
- payment_key (FK → dim_payment)
- passenger_count
- trip_distance
- fare_amount
- tip_amount
- total_amount
- trip_duration_minutes
- data_source_month (partition key)

**Dimension Tables:**
- dim_datetime: Time-based attributes
- dim_location: NYC taxi zones
- dim_payment: Payment types

## Deployment Patterns

### Local Mode

- Docker Compose orchestrates all services
- PostgreSQL simulates BigQuery
- Local filesystem replaces GCS
- Airflow runs in containers
- No GCP costs

### GCP Mode

- Terraform provisions infrastructure
- Cloud Composer orchestrates workflows
- Dataproc runs Spark jobs
- BigQuery stores warehouse
- VPC provides network isolation

## Performance Optimization

### BigQuery Optimizations

1. **Partitioning**: By data_source_month
2. **Clustering**: By datetime_key, pickup_location_key
3. **Column Pruning**: Only select needed columns
4. **Partition Pruning**: Filter by month in queries

### Spark Optimizations

1. **Adaptive Query Execution**: Enabled
2. **Partition Coalescing**: Dynamic partition management
3. **Broadcast Joins**: For small dimension tables
4. **Shuffle Partitions**: Tuned based on mode

## Security Implementation

### Network Security

- Private VPC with custom subnets
- Cloud NAT for outbound internet
- Firewall rules (deny all ingress)
- Private IP for Composer

### Access Control

- Service accounts with least privilege
- IAM roles scoped per service
- No public IPs on compute
- Encryption at rest and in transit

### Secret Management

- Environment variables for local
- Secret Manager for GCP
- No credentials in code/git

## Monitoring & Observability

### Metrics

- Pipeline execution time
- Data volume processed
- Error rates
- Cost tracking

### Logging

- Airflow task logs
- Spark application logs
- System logs (Cloud Logging)

### Alerting

- Pipeline failures
- Data quality issues
- Cost thresholds

## Disaster Recovery

### Backup Strategy

- GCS versioning enabled
- BigQuery time travel (7 days)
- Terraform state in GCS
- Automated snapshots

### Recovery Procedures

1. Identify failed month
2. Replay from bronze layer
3. Verify data quality
4. Update downstream systems

## Scalability

### Horizontal Scaling

- Dataproc: Add worker nodes
- Composer: Increase worker count
- BigQuery: Automatic scaling

### Data Volume

- Current: ~2M trips/month
- Designed for: 100M trips/month
- Storage: Petabyte-scale capable

## Cost Management

### Optimization Strategies

1. Preemptible workers (Dataproc)
2. Auto-pause Composer
3. Lifecycle policies (GCS)
4. Query optimization (BigQuery)
5. Scheduled infrastructure

### Cost Breakdown

- Composer: ~$300/month (main cost)
- Storage: Free tier eligible
- BigQuery: Pay per query
- Networking: Minimal
- Dataproc: Pay per use

## Future Enhancements

1. **Streaming**: Add real-time ingestion
2. **ML Integration**: Demand prediction models
3. **Data Quality**: Great Expectations framework
4. **Metadata**: Apache Atlas integration
5. **Visualization**: Looker/Tableau dashboards
6. **Multi-region**: Global deployment
7. **CDC**: Change data capture
8. **Schema Evolution**: Automatic handling
