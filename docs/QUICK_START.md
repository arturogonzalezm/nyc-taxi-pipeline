# Quick Start - Historical Data Ingestion

## 🚀 TL;DR

```bash
# Single month
python -m etl.jobs.ingestion.taxi_ingestion_job \
    --taxi-type yellow --year 2024 --month 1

# Date range (historical)
python -m etl.jobs.ingestion.taxi_ingestion_job \
    --taxi-type yellow \
    --start-year 2023 --start-month 1 \
    --end-year 2024 --end-month 12
```

## ✅ Both Requirements Met

### 1. Historical Data Ingestion ✅
- Single month: `--year 2024 --month 1`
- Date range: `--start-year 2023 --start-month 1 --end-year 2024 --end-month 12`
- Multi-year support
- Fault-tolerant

### 2. Raw Data Preservation ✅
- All original columns preserved
- Only metadata added (6 columns):
  - `ingestion_timestamp` - audit trail
  - `ingestion_date` - audit trail
  - `source_file` - lineage tracking
  - `record_hash` - CDC change detection
  - `year` - partition column
  - `month` - partition column
- Schema enforcement only (timestamp type)
- **NO business logic transformations**

## 📊 Data Flow

```
NYC TLC Source
     ↓
  Download
     ↓
MinIO Cache (optional)
     ↓
Validation (year/month)
     ↓
Add Metadata (6 columns)
     ↓
Bronze Layer
s3a://nyc-taxi-pipeline/bronze/nyc_taxi/{type}/year={year}/month={month}/
```

## 📁 Output Structure

```
s3a://nyc-taxi-pipeline/
├── bronze/
│   └── nyc_taxi/
│       ├── yellow/
│       │   ├── cache/                      # Download cache
│       │   │   ├── yellow_tripdata_2024-01.parquet
│       │   │   └── yellow_tripdata_2024-02.parquet
│       │   ├── year=2024/
│       │   │   ├── month=1/
│       │   │   │   └── part-*.parquet     # Raw data + metadata
│       │   │   ├── month=2/
│       │   │   │   └── part-*.parquet
│       │   │   └── month=3/
│       │   │       └── part-*.parquet
│       │   └── year=2023/
│       │       └── month=1/ through month=12/
│       └── green/
│           └── (same structure)
└── misc/
    └── taxi_zone_lookup.csv               # Reference data
```

## 🔍 What Gets Transformed?

### ✅ Transformations Applied (Metadata Only)
- Schema enforcement (timestamp type)
- Year/month filtering (data validation)
- Add 6 metadata columns

### ❌ NOT Applied (Zero Business Logic)
- ❌ No fare calculations
- ❌ No trip duration calculations
- ❌ No distance validations
- ❌ No data enrichment
- ❌ No aggregations
- ❌ No data cleansing

**Business logic happens in Silver/Gold layers!**

## 💡 Key Features

1. **MinIO-First Caching**
   - Downloads once, caches in MinIO
   - Faster re-runs
   - Shared across team

2. **Partitioned Storage**
   - By year and month
   - Delta Lake compatible
   - Query optimization

3. **Append Mode**
   - Never overwrites
   - Safe re-runs
   - Complete history

4. **Comprehensive Logging**
   - Every step logged
   - Data quality metrics
   - Execution timings

5. **Production-Ready**
   - Custom exceptions
   - Fault tolerance
   - Full type hints
   - Complete documentation

## 📖 Full Documentation

See [HISTORICAL_INGESTION_GUIDE.md](HISTORICAL_INGESTION_GUIDE.md) for:
- Detailed API documentation
- Real-world examples
- Schema comparison (original vs bronze)
- Caching strategy details
- Data quality validation
- Production features
