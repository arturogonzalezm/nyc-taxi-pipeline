-- NYC Taxi Dimensional Model Schema for PostgreSQL
-- Creates fact and dimension tables for the gold layer
-- Run order: 1 (executed automatically by docker-entrypoint-initdb.d)

-- Create schema
CREATE SCHEMA IF NOT EXISTS taxi;

-- Set search path
SET search_path TO taxi, public;

-- ============================================================================
-- Dimension Tables
-- ============================================================================

-- Dimension: Date
-- Grain: One row per calendar date
-- ~365-400 rows per year
CREATE TABLE IF NOT EXISTS dim_date
(
    date_key         INTEGER PRIMARY KEY,
    date             DATE        NOT NULL UNIQUE,
    year             INTEGER     NOT NULL,
    quarter          INTEGER     NOT NULL,
    month            INTEGER     NOT NULL,
    month_name       VARCHAR(20) NOT NULL,
    day              INTEGER     NOT NULL,
    day_of_week      INTEGER     NOT NULL,
    day_of_week_name VARCHAR(20) NOT NULL,
    is_weekend       BOOLEAN     NOT NULL,
    week_of_year     INTEGER     NOT NULL
);

CREATE INDEX idx_dim_date_date ON dim_date (date);
CREATE INDEX idx_dim_date_year_month ON dim_date (year, month);

COMMENT ON TABLE dim_date IS 'Date dimension with calendar attributes';
COMMENT ON COLUMN dim_date.date_key IS 'Surrogate key in YYYYMMDD format';
COMMENT ON COLUMN dim_date.is_weekend IS 'True if Saturday or Sunday';

-- Dimension: Location (NYC Taxi Zones)
-- Grain: One row per taxi zone
-- 265 zones (static reference data)
CREATE TABLE IF NOT EXISTS dim_location
(
    location_key INTEGER PRIMARY KEY,
    borough      VARCHAR(50)  NOT NULL,
    zone         VARCHAR(100) NOT NULL,
    service_zone VARCHAR(50)  NOT NULL
);

CREATE INDEX idx_dim_location_borough ON dim_location (borough);
CREATE INDEX idx_dim_location_service_zone ON dim_location (service_zone);

COMMENT ON TABLE dim_location IS 'NYC taxi zone dimension';
COMMENT ON COLUMN dim_location.location_key IS 'LocationID from NYC TLC';
COMMENT ON COLUMN dim_location.service_zone IS 'Yellow Zone, Boro Zone, or EWR';

-- Dimension: Payment
-- Grain: One row per (payment_type, rate_code) combination
-- ~20-30 rows
CREATE TABLE IF NOT EXISTS dim_payment
(
    payment_key       BIGINT PRIMARY KEY,
    payment_type_id   INTEGER      NOT NULL,
    payment_type_desc VARCHAR(50)  NOT NULL,
    rate_code_id      INTEGER      NOT NULL,
    rate_code_desc    VARCHAR(100) NOT NULL
);

CREATE INDEX idx_dim_payment_type ON dim_payment (payment_type_id);
CREATE INDEX idx_dim_payment_rate ON dim_payment (rate_code_id);

COMMENT ON TABLE dim_payment IS 'Payment type and rate code dimension';
COMMENT ON COLUMN dim_payment.payment_key IS 'Surrogate key (BIGINT)';

-- ============================================================================
-- Fact Table
-- ============================================================================

-- Fact: Trip
-- Grain: One row per taxi trip
-- ~20-40 million rows per month for yellow taxi
CREATE TABLE IF NOT EXISTS fact_trip
(
    trip_key              BIGINT PRIMARY KEY,
    date_key              INTEGER   NOT NULL,
    pickup_location_key   INTEGER   NOT NULL,
    dropoff_location_key  INTEGER   NOT NULL,
    payment_key           BIGINT    NOT NULL,
    pickup_datetime       TIMESTAMP NOT NULL,
    dropoff_datetime      TIMESTAMP NOT NULL,
    passenger_count       INTEGER,
    trip_distance         DOUBLE PRECISION,
    trip_duration_seconds INTEGER,
    trip_duration_minutes DOUBLE PRECISION,
    fare_amount           DOUBLE PRECISION,
    extra                 DOUBLE PRECISION,
    mta_tax               DOUBLE PRECISION,
    tip_amount            DOUBLE PRECISION,
    tolls_amount          DOUBLE PRECISION,
    total_amount          DOUBLE PRECISION,
    tip_percentage        DOUBLE PRECISION,
    avg_speed_mph         DOUBLE PRECISION,
    partition_year        INTEGER   NOT NULL,
    partition_month       INTEGER   NOT NULL,

    -- Foreign key constraints
    CONSTRAINT fk_fact_trip_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_trip_pickup_location FOREIGN KEY (pickup_location_key) REFERENCES dim_location (location_key),
    CONSTRAINT fk_fact_trip_dropoff_location FOREIGN KEY (dropoff_location_key) REFERENCES dim_location (location_key),
    CONSTRAINT fk_fact_trip_payment FOREIGN KEY (payment_key) REFERENCES dim_payment (payment_key)
);

-- Indexes for query performance
CREATE INDEX idx_fact_trip_date_key ON fact_trip (date_key);
CREATE INDEX idx_fact_trip_pickup_location ON fact_trip (pickup_location_key);
CREATE INDEX idx_fact_trip_dropoff_location ON fact_trip (dropoff_location_key);
CREATE INDEX idx_fact_trip_payment_key ON fact_trip (payment_key);
CREATE INDEX idx_fact_trip_pickup_datetime ON fact_trip (pickup_datetime);
CREATE INDEX idx_fact_trip_partition ON fact_trip (partition_year, partition_month);

-- Composite indexes for common queries
CREATE INDEX idx_fact_trip_date_pickup_loc ON fact_trip (date_key, pickup_location_key);
CREATE INDEX idx_fact_trip_date_payment ON fact_trip (date_key, payment_key);

COMMENT ON TABLE fact_trip IS 'Fact table containing taxi trip transactions';
COMMENT ON COLUMN fact_trip.trip_key IS 'Surrogate key (BIGINT for large datasets)';
COMMENT ON COLUMN fact_trip.trip_distance IS 'Trip distance in miles';
COMMENT ON COLUMN fact_trip.trip_duration_seconds IS 'Duration in seconds';
COMMENT ON COLUMN fact_trip.total_amount IS 'Total fare in USD';

-- ============================================================================
-- Partitioning (PostgreSQL 10+)
-- ============================================================================

-- Note: Partitioning is optional for smaller datasets
-- For large datasets (>100M rows), consider partitioning fact_trip by year/month
-- Example (commented out):
/*
DROP TABLE IF EXISTS fact_trip;

CREATE TABLE fact_trip (
    trip_key BIGINT,
    date_key INTEGER NOT NULL,
    -- ... other columns ...
    partition_year INTEGER NOT NULL,
    partition_month INTEGER NOT NULL,
    PRIMARY KEY (trip_key, partition_year, partition_month)
) PARTITION BY RANGE (partition_year, partition_month);

CREATE TABLE fact_trip_2024_01 PARTITION OF fact_trip
    FOR VALUES FROM (2024, 1) TO (2024, 2);

CREATE TABLE fact_trip_2024_02 PARTITION OF fact_trip
    FOR VALUES FROM (2024, 2) TO (2024, 3);
*/

-- ============================================================================
-- Statistics and Maintenance
-- ============================================================================

-- Analyze tables for query optimization
ANALYZE dim_date;
ANALYZE dim_location;
ANALYZE dim_payment;
ANALYZE fact_trip;

-- Grant permissions
GRANT USAGE ON SCHEMA taxi TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA taxi TO PUBLIC;
