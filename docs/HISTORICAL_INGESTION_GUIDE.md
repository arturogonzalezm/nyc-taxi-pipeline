# NYC Taxi Historical Data Ingestion - Complete Guide

## ✅ Requirements Met

### 1. Historical Data Ingestion Support
- ✅ Single month ingestion
- ✅ Bulk date range ingestion (multiple months/years)
- ✅ Command-line and programmatic APIs
- ✅ Fault-tolerant (continues on individual month failures)

### 2. Raw Data Preservation
- ✅ Preserves all original columns unchanged
- ✅ Schema enforcement only (timestamp type validation)
- ✅ Metadata columns added (lineage, CDC tracking)
- ✅ **NO business logic transformations**
- ✅ Partitioned storage for Delta Lake compatibility

---

## Usage Examples

### Single Month Ingestion

#### Method 1: Command Line
```bash
# Ingest yellow taxi data for January 2024
python -m etl.jobs.ingestion.taxi_ingestion_job \
    --taxi-type yellow \
    --year 2024 \
    --month 1

# Ingest green taxi data for February 2024
python -m etl.jobs.ingestion.taxi_ingestion_job \
    --taxi-type green \
    --year 2024 \
    --month 2
```

#### Method 2: Python Function
```python
from etl.jobs.ingestion.taxi_ingestion_job import run_ingestion

# Ingest single month
success = run_ingestion(
    taxi_type="yellow",
    year=2024,
    month=1
)

if success:
    print("Ingestion completed successfully!")
```

#### Method 3: Python Class (with metrics)
```python
from etl.jobs.ingestion.taxi_ingestion_job import TaxiIngestionJob

# Create and run job
job = TaxiIngestionJob(
    taxi_type="yellow",
    year=2024,
    month=1
)

success = job.run()

# Get execution metrics
metrics = job.get_metrics()
print(f"Extract time: {metrics['extract_duration_seconds']}s")
print(f"Transform time: {metrics['transform_duration_seconds']}s")
print(f"Load time: {metrics['load_duration_seconds']}s")
print(f"Total time: {metrics['total_duration_seconds']}s")
```

**Output Location:**
```
s3a://nyc-taxi-pipeline/bronze/nyc_taxi/yellow/year=2024/month=1/*.parquet
```

---

### Bulk Historical Ingestion (Date Range)

#### Method 1: Command Line

```bash
# Ingest full year 2023 (12 months)
python -m etl.jobs.ingestion.taxi_ingestion_job \
    --taxi-type yellow \
    --start-year 2023 \
    --start-month 1 \
    --end-year 2023 \
    --end-month 12

# Ingest multiple years (2020-2024)
python -m etl.jobs.ingestion.taxi_ingestion_job \
    --taxi-type yellow \
    --start-year 2020 \
    --start-month 1 \
    --end-year 2024 \
    --end-month 12

# Ingest partial year (Jan 2024 to Jun 2024)
python -m etl.jobs.ingestion.taxi_ingestion_job \
    --taxi-type green \
    --start-year 2024 \
    --start-month 1 \
    --end-year 2024 \
    --end-month 6
```

#### Method 2: Python Function
```python
from etl.jobs.ingestion.taxi_ingestion_job import run_bulk_ingestion

# Ingest date range
results = run_bulk_ingestion(
    taxi_type="yellow",
    start_year=2023,
    start_month=1,
    end_year=2023,
    end_month=12
)

# Check results for each month
for period, status in results.items():
    print(f"{period}: {status}")

# Count successes and failures
successful = sum(1 for s in results.values() if s == "SUCCESS")
failed = len(results) - successful

print(f"\nTotal: {len(results)} months")
print(f"Successful: {successful}")
print(f"Failed: {failed}")
```

**Example Output:**
```
2023-01: SUCCESS
2023-02: SUCCESS
2023-03: SUCCESS
2023-04: SUCCESS
2023-05: SUCCESS
2023-06: SUCCESS
2023-07: SUCCESS
2023-08: SUCCESS
2023-09: SUCCESS
2023-10: SUCCESS
2023-11: SUCCESS
2023-12: SUCCESS

Total: 12 months
Successful: 12
Failed: 0
```

**Output Structure:**
```
s3a://nyc-taxi-pipeline/bronze/nyc_taxi/yellow/
├── year=2023/
│   ├── month=1/
│   │   └── part-*.parquet
│   ├── month=2/
│   │   └── part-*.parquet
│   ├── month=3/
│   │   └── part-*.parquet
│   ...
│   └── month=12/
│       └── part-*.parquet
```

---

## Raw Data Preservation - Bronze Layer Principles

### Transformations Applied

The ingestion pipeline follows **Bronze Layer** best practices - preserving raw data with minimal transformation:

#### ✅ What IS Applied (Metadata Only)

1. **Schema Enforcement**
   - Cast `pickup_datetime` to `TimestampType` (if needed)
   - Ensures data types are consistent

2. **Data Validation**
   - Extract year/month from pickup datetime
   - Filter to only include records matching requested period
   - Raise error if no matching records found
   - Log data distribution and match percentage

3. **Metadata Addition** (NEW columns)
   - `ingestion_timestamp`: When data was ingested (for audit trail)
   - `ingestion_date`: Date of ingestion
   - `source_file`: Original filename (e.g., `yellow_tripdata_2024-01.parquet`)
   - `record_hash`: Hash of all columns (for CDC change detection)
   - `year`: Partition column
   - `month`: Partition column

#### ❌ What is NOT Applied (Business Logic)

- ❌ No fare calculations or validations
- ❌ No trip duration calculations
- ❌ No distance validations
- ❌ No data enrichment (zone lookups, etc.)
- ❌ No aggregations or summarizations
- ❌ No data cleansing or corrections

**These transformations happen in Silver/Gold layers!**

### Schema Comparison

#### Original Schema (from NYC TLC)
```
VendorID
tpep_pickup_datetime
tpep_dropoff_datetime
passenger_count
trip_distance
RatecodeID
store_and_fwd_flag
PULocationID
DOLocationID
payment_type
fare_amount
extra
mta_tax
tip_amount
tolls_amount
improvement_surcharge
total_amount
congestion_surcharge
```

#### Bronze Schema (after ingestion)
```
VendorID                    [UNCHANGED]
tpep_pickup_datetime        [UNCHANGED - type enforced to TimestampType]
tpep_dropoff_datetime       [UNCHANGED]
passenger_count             [UNCHANGED]
trip_distance               [UNCHANGED]
RatecodeID                  [UNCHANGED]
store_and_fwd_flag          [UNCHANGED]
PULocationID                [UNCHANGED]
DOLocationID                [UNCHANGED]
payment_type                [UNCHANGED]
fare_amount                 [UNCHANGED]
extra                       [UNCHANGED]
mta_tax                     [UNCHANGED]
tip_amount                  [UNCHANGED]
tolls_amount                [UNCHANGED]
improvement_surcharge       [UNCHANGED]
total_amount                [UNCHANGED]
congestion_surcharge        [UNCHANGED]
ingestion_timestamp         [NEW - metadata]
ingestion_date              [NEW - metadata]
source_file                 [NEW - metadata]
record_hash                 [NEW - for CDC]
year                        [NEW - partition column]
month                       [NEW - partition column]
```

**Result:** All original data preserved + 6 metadata columns

---

## Storage Strategy

### Partitioning

Data is partitioned by `year` and `month` for:
- ✅ **Delta Lake compatibility** - Supports incremental updates
- ✅ **CDC operations** - Change Data Capture tracking
- ✅ **Query optimization** - Partition pruning for faster queries
- ✅ **Data organization** - Logical separation by time period

### Append Mode

Data is written in **append mode** which means:
- ✅ Never overwrites existing data
- ✅ Supports incremental ingestion
- ✅ Safe for re-running failed months
- ✅ Maintains complete historical record

### Compression

Uses **Snappy compression** for:
- ✅ Fast compression/decompression
- ✅ Splittable format (parallelizable reads)
- ✅ Good balance of compression ratio vs speed

---

## Caching Strategy

The pipeline implements a **MinIO-first caching strategy** for efficiency:

### Cache Flow

1. **Check MinIO cache** at `s3a://nyc-taxi-pipeline/bronze/nyc_taxi/{taxi_type}/cache/`
2. If **cache hit** → Load directly from MinIO (fast)
3. If **cache miss** → Download from NYC TLC
4. **Upload to MinIO cache** for future use
5. **Fallback to local cache** if MinIO unavailable

### Cache Locations

**MinIO Cache:**
```
s3a://nyc-taxi-pipeline/bronze/nyc_taxi/yellow/cache/yellow_tripdata_2024-01.parquet
s3a://nyc-taxi-pipeline/bronze/nyc_taxi/green/cache/green_tripdata_2024-01.parquet
```

**Local Cache (fallback):**
```
/tmp/nyc-taxi-pipeline/cache/yellow_tripdata_2024-01.parquet
(or system temp directory on Windows/Linux)
```

### Benefits

- ✅ **Faster re-runs** - No need to re-download
- ✅ **Reduced bandwidth** - Downloads only once
- ✅ **Shared cache** - Team members benefit from each other's downloads
- ✅ **Offline development** - Local cache allows working offline

---

## Data Quality & Validation

### Validations Applied

1. **Year/Month Match Validation**
   - Extracts year and month from `pickup_datetime`
   - Filters to only requested period
   - Raises error if 0 records match
   - Warns if <50% of records match requested period

2. **Schema Validation**
   - Checks for required columns (`pickup_datetime`)
   - Enforces timestamp data type
   - Logs schema for audit trail

3. **Data Distribution Logging**
   - Logs count by year/month before filtering
   - Logs match percentage
   - Logs final record count after filtering

### Error Handling

- **DownloadError** - If file cannot be downloaded from NYC TLC
- **DataValidationError** - If data doesn't match requested period
- **JobExecutionError** - If any step fails

All errors include detailed context for debugging.

---

## Production Features

### Metrics Tracking

Every job execution tracks:
- `extract_duration_seconds` - Time to download/load data
- `transform_duration_seconds` - Time to validate and add metadata
- `load_duration_seconds` - Time to write to MinIO
- `total_duration_seconds` - Total job execution time

Access via:
```python
job = TaxiIngestionJob("yellow", 2024, 1)
job.run()
metrics = job.get_metrics()
```

### Logging

Comprehensive logging at all levels:
- **INFO** - Normal operation steps
- **WARNING** - Non-critical issues (low match percentage, cache failures)
- **ERROR** - Critical failures with full context

### Fault Tolerance

Bulk ingestion is fault-tolerant:
- ✅ Each month processed independently
- ✅ One failure doesn't stop processing
- ✅ Detailed results for each month
- ✅ Summary report at end

---

## Real-World Examples

### Example 1: Backfill Historical Data (5 years)

```bash
# Ingest 5 years of yellow taxi data (2019-2023)
python -m etl.jobs.ingestion.taxi_ingestion_job \
    --taxi-type yellow \
    --start-year 2019 \
    --start-month 1 \
    --end-year 2023 \
    --end-month 12
```

**Result:** 60 months (5 years × 12 months) ingested with partitioning:
```
s3a://nyc-taxi-pipeline/bronze/nyc_taxi/yellow/
├── year=2019/month=1/ through month=12/
├── year=2020/month=1/ through month=12/
├── year=2021/month=1/ through month=12/
├── year=2022/month=1/ through month=12/
└── year=2023/month=1/ through month=12/
```

### Example 2: Incremental Monthly Update

```python
from etl.jobs.ingestion.taxi_ingestion_job import run_ingestion
from datetime import datetime

# Get current year and month
now = datetime.now()
current_year = now.year
current_month = now.month

# Ingest latest month
success = run_ingestion("yellow", current_year, current_month)

if success:
    print(f"Successfully ingested {current_year}-{current_month:02d}")
```

### Example 3: Re-ingest Failed Months

```python
from etl.jobs.ingestion.taxi_ingestion_job import run_ingestion

# List of months that failed previously
failed_months = [
    (2023, 3),
    (2023, 7),
    (2023, 11)
]

# Re-ingest each failed month
for year, month in failed_months:
    print(f"Re-ingesting {year}-{month:02d}...")
    success = run_ingestion("yellow", year, month)
    print(f"  {'SUCCESS' if success else 'FAILED'}")
```

---

## Summary

### ✅ Requirement 1: Historical Data Ingestion

| Feature | Status | Notes |
|---------|--------|-------|
| Single month ingestion | ✅ | Via CLI or Python API |
| Date range ingestion | ✅ | Start/end year and month |
| Multi-year ingestion | ✅ | Handles any date range |
| Fault tolerance | ✅ | Continues on individual failures |
| Progress tracking | ✅ | Returns results per month |
| Command-line interface | ✅ | Full argument support |
| Programmatic API | ✅ | Function and class interfaces |

### ✅ Requirement 2: Raw Data Preservation

| Feature | Status | Notes |
|---------|--------|-------|
| Original columns preserved | ✅ | 100% of source data retained |
| Schema enforcement | ✅ | Timestamp type validation only |
| Metadata columns added | ✅ | 6 columns for lineage/CDC |
| No business logic | ✅ | Zero transformations applied |
| Partitioned storage | ✅ | Year/month for Delta Lake |
| Append mode | ✅ | Never overwrites existing data |
| Compression | ✅ | Snappy format |
| Data validation | ✅ | Year/month filtering only |

---

## Next Steps

After bronze layer ingestion, data flows to:

1. **Silver Layer** - Business logic transformations
   - Calculate trip duration
   - Validate fare amounts
   - Enrich with zone lookups
   - Clean and standardize data

2. **Gold Layer** - Aggregations and analytics
   - Daily/monthly aggregations
   - Zone-based analytics
   - Performance metrics
   - Business KPIs

The bronze layer provides the **immutable foundation** for all downstream processing!
