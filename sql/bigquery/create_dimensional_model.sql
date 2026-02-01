-- NYC Taxi Dimensional Model Schema for BigQuery
-- Creates fact and dimension tables for the gold layer
-- Run order: 1 (execute manually or via BigQuery console/CLI)

-- ============================================================================
-- Dataset Creation (run separately if needed)
-- ============================================================================
-- CREATE SCHEMA IF NOT EXISTS taxi OPTIONS(location = 'US');

-- ============================================================================
-- Dimension Tables
-- ============================================================================

-- Dimension: Date
-- Grain: One row per calendar date
-- ~365-400 rows per year
CREATE TABLE IF NOT EXISTS taxi.dim_date
(
    date_key         INT64 NOT NULL,
    date             DATE NOT NULL,
    year             INT64 NOT NULL,
    quarter          INT64 NOT NULL,
    month            INT64 NOT NULL,
    month_name       STRING NOT NULL,
    day              INT64 NOT NULL,
    day_of_week      INT64 NOT NULL,
    day_of_week_name STRING NOT NULL,
    is_weekend       BOOL NOT NULL,
    week_of_year     INT64 NOT NULL,
    -- Gold layer metadata
    created_timestamp TIMESTAMP NOT NULL,
    data_layer       STRING NOT NULL,
    -- Load layer metadata
    bigquery_load_timestamp TIMESTAMP NOT NULL,
    bigquery_load_date      DATE NOT NULL,
    load_job_name           STRING
)
OPTIONS(
    description = 'Date dimension with calendar attributes'
);

-- Dimension: Location (NYC Taxi Zones)
-- Grain: One row per taxi zone
-- 265 zones (static reference data)
CREATE TABLE IF NOT EXISTS taxi.dim_location
(
    location_key INT64 NOT NULL,
    borough      STRING NOT NULL,
    zone         STRING NOT NULL,
    service_zone STRING NOT NULL,
    -- Gold layer metadata
    created_timestamp TIMESTAMP NOT NULL,
    data_layer       STRING NOT NULL,
    -- Load layer metadata
    bigquery_load_timestamp TIMESTAMP NOT NULL,
    bigquery_load_date      DATE NOT NULL,
    load_job_name           STRING
)
OPTIONS(
    description = 'NYC taxi zone dimension'
);

-- Dimension: Payment
-- Grain: One row per (payment_type, rate_code) combination
-- ~20-30 rows
CREATE TABLE IF NOT EXISTS taxi.dim_payment
(
    payment_key       INT64 NOT NULL,
    payment_type_id   INT64 NOT NULL,
    payment_type_desc STRING NOT NULL,
    rate_code_id      INT64 NOT NULL,
    rate_code_desc    STRING NOT NULL,
    -- Gold layer metadata
    created_timestamp TIMESTAMP NOT NULL,
    data_layer       STRING NOT NULL,
    -- Load layer metadata
    bigquery_load_timestamp TIMESTAMP NOT NULL,
    bigquery_load_date      DATE NOT NULL,
    load_job_name           STRING
)
OPTIONS(
    description = 'Payment type and rate code dimension'
);

-- ============================================================================
-- Fact Table
-- ============================================================================

-- Fact: Trip
-- Grain: One row per taxi trip
-- ~20-40 million rows per month for yellow taxi
CREATE TABLE IF NOT EXISTS taxi.fact_trip
(
    trip_key              INT64 NOT NULL,
    date_key              INT64 NOT NULL,
    pickup_location_key   INT64 NOT NULL,
    dropoff_location_key  INT64 NOT NULL,
    payment_key           INT64 NOT NULL,
    pickup_datetime       TIMESTAMP NOT NULL,
    dropoff_datetime      TIMESTAMP NOT NULL,
    passenger_count       INT64,
    trip_distance         FLOAT64,
    trip_duration_seconds INT64,
    trip_duration_minutes FLOAT64,
    fare_amount           FLOAT64,
    extra                 FLOAT64,
    mta_tax               FLOAT64,
    tip_amount            FLOAT64,
    tolls_amount          FLOAT64,
    total_amount          FLOAT64,
    tip_percentage        FLOAT64,
    avg_speed_mph         FLOAT64,
    partition_year        INT64 NOT NULL,
    partition_month       INT64 NOT NULL,
    -- Gold layer metadata
    gold_transformation_timestamp TIMESTAMP NOT NULL,
    gold_transformation_date      DATE NOT NULL,
    gold_job_name                 STRING,
    data_layer                    STRING NOT NULL,
    -- Load layer metadata
    bigquery_load_timestamp TIMESTAMP NOT NULL,
    bigquery_load_date      DATE NOT NULL,
    load_job_name           STRING,
    -- Hash for idempotency and data integrity
    fact_hash             STRING NOT NULL
)
PARTITION BY DATE_TRUNC(pickup_datetime, MONTH)
CLUSTER BY date_key, pickup_location_key, dropoff_location_key
OPTIONS(
    description = 'Fact table containing taxi trip transactions',
    require_partition_filter = false
);

-- ============================================================================
-- Views for Common Queries
-- ============================================================================

-- View: Trip Summary by Date
CREATE OR REPLACE VIEW taxi.v_trip_summary_by_date AS
SELECT
    d.date,
    d.year,
    d.month,
    d.month_name,
    d.day_of_week_name,
    d.is_weekend,
    COUNT(*) AS trip_count,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.total_amount) AS avg_fare,
    AVG(f.trip_distance) AS avg_distance,
    AVG(f.trip_duration_minutes) AS avg_duration_minutes,
    AVG(f.tip_percentage) AS avg_tip_percentage
FROM taxi.fact_trip f
JOIN taxi.dim_date d ON f.date_key = d.date_key
GROUP BY d.date, d.year, d.month, d.month_name, d.day_of_week_name, d.is_weekend;

-- View: Trip Summary by Location
CREATE OR REPLACE VIEW taxi.v_trip_summary_by_location AS
SELECT
    pl.borough AS pickup_borough,
    pl.zone AS pickup_zone,
    dl.borough AS dropoff_borough,
    dl.zone AS dropoff_zone,
    COUNT(*) AS trip_count,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.total_amount) AS avg_fare,
    AVG(f.trip_distance) AS avg_distance
FROM taxi.fact_trip f
JOIN taxi.dim_location pl ON f.pickup_location_key = pl.location_key
JOIN taxi.dim_location dl ON f.dropoff_location_key = dl.location_key
GROUP BY pl.borough, pl.zone, dl.borough, dl.zone;

-- View: Payment Analysis
CREATE OR REPLACE VIEW taxi.v_payment_analysis AS
SELECT
    p.payment_type_desc,
    p.rate_code_desc,
    COUNT(*) AS trip_count,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.tip_percentage) AS avg_tip_percentage,
    AVG(f.total_amount) AS avg_fare
FROM taxi.fact_trip f
JOIN taxi.dim_payment p ON f.payment_key = p.payment_key
GROUP BY p.payment_type_desc, p.rate_code_desc;
