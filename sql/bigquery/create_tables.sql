-- BigQuery DDL for NYC Taxi Data Warehouse
-- Note: These tables are created via Terraform, but this file documents the schema

-- Fact table: trips
CREATE TABLE IF NOT EXISTS `PROJECT_ID.nyc_taxi_warehouse.fact_trips`
(
  trip_id INT64 NOT NULL,
  datetime_key INT64 NOT NULL,
  pickup_location_key INT64 NOT NULL,
  dropoff_location_key INT64 NOT NULL,
  payment_key INT64 NOT NULL,
  passenger_count INT64,
  trip_distance FLOAT64,
  fare_amount FLOAT64,
  tip_amount FLOAT64,
  total_amount FLOAT64,
  trip_duration_minutes FLOAT64,
  data_source_month STRING NOT NULL
)
PARTITION BY data_source_month
CLUSTER BY datetime_key, pickup_location_key
OPTIONS(
  description="Fact table containing NYC taxi trip details",
  labels=[("table_type", "fact"), ("project", "nyc-taxi-pipeline")]
);

-- Dimension table: datetime
CREATE TABLE IF NOT EXISTS `PROJECT_ID.nyc_taxi_warehouse.dim_datetime`
(
  datetime_key INT64 NOT NULL,
  pickup_datetime TIMESTAMP NOT NULL,
  year INT64,
  month INT64,
  day INT64,
  hour INT64,
  minute INT64,
  day_of_week INT64,
  is_weekend BOOL,
  is_holiday BOOL
)
OPTIONS(
  description="Datetime dimension with temporal attributes",
  labels=[("table_type", "dimension"), ("project", "nyc-taxi-pipeline")]
);

-- Dimension table: location
CREATE TABLE IF NOT EXISTS `PROJECT_ID.nyc_taxi_warehouse.dim_location`
(
  location_key INT64 NOT NULL,
  location_id INT64 NOT NULL,
  borough STRING,
  zone STRING,
  service_zone STRING
)
OPTIONS(
  description="Location dimension with NYC taxi zones",
  labels=[("table_type", "dimension"), ("project", "nyc-taxi-pipeline")]
);

-- Dimension table: payment
CREATE TABLE IF NOT EXISTS `PROJECT_ID.nyc_taxi_warehouse.dim_payment`
(
  payment_key INT64 NOT NULL,
  payment_name STRING NOT NULL,
  is_card BOOL
)
OPTIONS(
  description="Payment type dimension",
  labels=[("table_type", "dimension"), ("project", "nyc-taxi-pipeline")]
);
