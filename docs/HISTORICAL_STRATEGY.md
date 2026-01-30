# Historical Data Strategy

## Overview

The pipeline implements a robust strategy for handling historical data ingestion, backfills, and reprocessing. This ensures data consistency, prevents duplicates, and supports incremental updates.

## SCD Type 1 (Slowly Changing Dimension Type 1)

This pipeline uses **SCD Type 1** for dimension management, which means:

- **Overwrites**: When a dimension record changes, the old value is overwritten with the new value
- **No History**: Previous values are not preserved; only the current state is stored
- **Simplicity**: Easiest to implement and maintain
- **Use Case**: Appropriate when historical dimension values are not needed for analysis

### Why SCD Type 1?

For NYC taxi data, dimension attributes (zones, payment types) rarely change, and when they do, historical accuracy of the old values is not critical for trip analysis. The focus is on current operational reporting rather than tracking how dimensions evolved over time.

## Key Principles

1. **Idempotency**: Running the same job multiple times produces the same result
2. **Deduplication**: Record hashing prevents duplicate data
3. **Incremental Processing**: Only process new or changed data
4. **Safe Backfills**: Re-process historical data without data loss

## Record Hashing

Each record is assigned a unique hash based on its key fields:

```python
record_hash = SHA256(
    taxi_type +
    pickup_datetime +
    dropoff_datetime +
    pickup_location_id +
    dropoff_location_id +
    trip_distance +
    fare_amount +
    total_amount
)
```

This hash serves as:
- Primary key for deduplication
- Change detection mechanism
- Audit trail identifier

## Ingestion Strategies

### Standard Ingestion

For regular monthly data loads:

```bash
python -m etl.jobs.bronze.taxi_ingestion_job \
    --taxi-type yellow \
    --year 2024 \
    --month 1
```

**Behavior:**
- Downloads parquet file from NYC TLC
- Adds metadata columns (hash, timestamp, source)
- Writes to Bronze layer with overwrite mode
- Partitioned by taxi_type/year/month

### Bulk Ingestion

For loading multiple months at once:

```bash
python -m etl.jobs.bronze.taxi_ingestion_job \
    --taxi-type yellow \
    --start-year 2023 --start-month 1 \
    --end-year 2023 --end-month 12
```

**Behavior:**
- Iterates through date range
- Processes each month sequentially
- Continues on failure (logs errors)

### Safe Backfill

For re-processing historical data without duplicates:

```bash
# Specific months
python etl/jobs/bronze/taxi_injection_safe_backfill_job.py yellow 2023-03 2023-07

# Date range
python etl/jobs/bronze/taxi_injection_safe_backfill_job.py yellow 2023-01:2023-12

# Skip deletion (keep existing)
python etl/jobs/bronze/taxi_injection_safe_backfill_job.py yellow 2024-01 --no-delete
```

**Behavior:**
1. Identifies target partitions
2. Optionally deletes existing data
3. Re-ingests from source
4. Validates record counts

## Load Strategy (PostgreSQL)

### Idempotent Upserts

The load job uses `INSERT ON CONFLICT` for idempotent writes:

```sql
INSERT INTO taxi.fact_trip (trip_id, ...)
VALUES (...)
ON CONFLICT (trip_id) DO UPDATE SET
    fare_amount = EXCLUDED.fare_amount,
    total_amount = EXCLUDED.total_amount,
    updated_at = NOW();
```

**Benefits:**
- Safe to re-run without duplicates
- Updates changed records
- Preserves audit timestamps

### Dimension Loading

Dimensions are loaded with similar upsert logic:

1. **dim_date**: Generated for date range, upserted
2. **dim_location**: Loaded from zone lookup, upserted
3. **dim_payment**: Static reference data, upserted

## Partition Management

### Bronze Layer Partitioning

```
s3://nyc-taxi-pipeline/bronze/
├── taxi_type=yellow/
│   ├── year=2024/
│   │   ├── month=1/
│   │   ├── month=2/
│   │   └── ...
│   └── year=2023/
│       └── ...
└── taxi_type=green/
    └── ...
```

### Overwrite Modes

| Mode | Use Case | Behavior |
|------|----------|----------|
| `overwrite` | Standard ingestion | Replace entire partition |
| `append` | Incremental loads | Add to existing partition |
| `dynamic` | Selective updates | Overwrite only affected partitions |

## Error Handling

### Retry Logic

Failed ingestions are retried with exponential backoff:

```python
max_retries = 3
retry_delay = [30, 60, 120]  # seconds
```

### Failure Recovery

1. **Partial Failures**: Log and continue with next partition
2. **Complete Failures**: Rollback and alert
3. **Data Validation**: Check record counts post-load

## Monitoring

### Metrics Tracked

- Records ingested per partition
- Processing time per job
- Error counts and types
- Data freshness (last successful load)

### Audit Columns

Every record includes:

| Column | Description |
|--------|-------------|
| `record_hash` | Unique identifier |
| `ingestion_timestamp` | When record was ingested |
| `source_file` | Original source file |
| `processing_timestamp` | When record was transformed |

## Best Practices

### Do's

- ✅ Use safe backfill for historical reprocessing
- ✅ Validate record counts after loads
- ✅ Monitor for duplicate records
- ✅ Use partitioning for efficient queries

### Don'ts

- ❌ Delete partitions manually without backfill
- ❌ Run multiple jobs on same partition simultaneously
- ❌ Skip validation steps
- ❌ Ignore failed job alerts
