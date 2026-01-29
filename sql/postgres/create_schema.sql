-- PostgreSQL Schema for NYC Taxi Data Warehouse
-- Star schema design for local mode

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS public;

-- Drop existing tables (for clean setup)
DROP TABLE IF EXISTS fact_trips CASCADE;
DROP TABLE IF EXISTS dim_datetime CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_payment CASCADE;

-- Dimension: Datetime
CREATE TABLE dim_datetime (
    datetime_key BIGINT PRIMARY KEY,
    pickup_datetime TIMESTAMP NOT NULL,
    year INT,
    month INT,
    day INT,
    hour INT,
    minute INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Dimension: Location
CREATE TABLE dim_location (
    location_key BIGINT PRIMARY KEY,
    location_id BIGINT NOT NULL,
    borough VARCHAR(50),
    zone VARCHAR(100),
    service_zone VARCHAR(50)
);

-- Dimension: Payment
CREATE TABLE dim_payment (
    payment_key BIGINT PRIMARY KEY,
    payment_name VARCHAR(50) NOT NULL,
    is_card BOOLEAN
);

-- Fact: Trips
CREATE TABLE fact_trips (
    trip_id BIGINT PRIMARY KEY,
    datetime_key BIGINT NOT NULL,
    pickup_location_key BIGINT NOT NULL,
    dropoff_location_key BIGINT NOT NULL,
    payment_key BIGINT NOT NULL,
    passenger_count BIGINT,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    trip_duration_minutes DOUBLE PRECISION,
    data_source_month VARCHAR(10) NOT NULL
);

-- Create indexes for fact table
CREATE INDEX idx_fact_trips_datetime ON fact_trips(datetime_key);
CREATE INDEX idx_fact_trips_pickup_location ON fact_trips(pickup_location_key);
CREATE INDEX idx_fact_trips_dropoff_location ON fact_trips(dropoff_location_key);
CREATE INDEX idx_fact_trips_payment ON fact_trips(payment_key);
CREATE INDEX idx_fact_trips_month ON fact_trips(data_source_month);

-- Create indexes for dimension tables
CREATE INDEX idx_dim_datetime_date ON dim_datetime(pickup_datetime);
CREATE INDEX idx_dim_datetime_year_month ON dim_datetime(year, month);
CREATE INDEX idx_dim_location_id ON dim_location(location_id);

-- Add foreign key constraints (optional, for data integrity)
-- Note: These are disabled by default for faster bulk loading
-- Uncomment to enable referential integrity checks

-- ALTER TABLE fact_trips
--     ADD CONSTRAINT fk_datetime FOREIGN KEY (datetime_key)
--     REFERENCES dim_datetime(datetime_key);

-- ALTER TABLE fact_trips
--     ADD CONSTRAINT fk_pickup_location FOREIGN KEY (pickup_location_key)
--     REFERENCES dim_location(location_key);

-- ALTER TABLE fact_trips
--     ADD CONSTRAINT fk_dropoff_location FOREIGN KEY (dropoff_location_key)
--     REFERENCES dim_location(location_key);

-- ALTER TABLE fact_trips
--     ADD CONSTRAINT fk_payment FOREIGN KEY (payment_key)
--     REFERENCES dim_payment(payment_key);

-- Create materialized view for common aggregations
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_trip_summary AS
SELECT
    d.year,
    d.month,
    d.day,
    d.is_weekend,
    COUNT(*) as total_trips,
    SUM(f.passenger_count) as total_passengers,
    ROUND(AVG(f.trip_distance)::numeric, 2) as avg_trip_distance,
    ROUND(AVG(f.fare_amount)::numeric, 2) as avg_fare_amount,
    ROUND(SUM(f.total_amount)::numeric, 2) as total_revenue,
    ROUND(AVG(f.trip_duration_minutes)::numeric, 2) as avg_trip_duration
FROM fact_trips f
JOIN dim_datetime d ON f.datetime_key = d.datetime_key
GROUP BY d.year, d.month, d.day, d.is_weekend;

CREATE INDEX idx_mv_daily_summary_date ON mv_daily_trip_summary(year, month, day);

-- Grant permissions (adjust as needed)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO PUBLIC;

-- Display table information
\echo 'Schema created successfully!'
\echo 'Tables created:'
\dt

\echo 'Indexes created:'
\di
