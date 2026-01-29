-- Sample Analytics Queries for NYC Taxi Data Warehouse (PostgreSQL)

-- Query 1: Basic trip count by month
SELECT
    data_source_month,
    COUNT(*) as total_trips,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance,
    ROUND(AVG(total_amount)::numeric, 2) as avg_fare
FROM fact_trips
GROUP BY data_source_month
ORDER BY data_source_month;

-- Query 2: Trips by hour of day
SELECT
    d.hour,
    COUNT(*) as trip_count,
    ROUND(AVG(f.total_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(f.trip_duration_minutes)::numeric, 2) as avg_duration
FROM fact_trips f
JOIN dim_datetime d ON f.datetime_key = d.datetime_key
GROUP BY d.hour
ORDER BY d.hour;

-- Query 3: Weekend vs Weekday analysis
SELECT
    d.is_weekend,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(*) as trip_count,
    ROUND(AVG(f.total_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(f.tip_amount)::numeric, 2) as avg_tip,
    ROUND(AVG(f.trip_distance)::numeric, 2) as avg_distance
FROM fact_trips f
JOIN dim_datetime d ON f.datetime_key = d.datetime_key
GROUP BY d.is_weekend
ORDER BY d.is_weekend;

-- Query 4: Payment type analysis
SELECT
    p.payment_name,
    COUNT(*) as trip_count,
    ROUND(AVG(f.total_amount)::numeric, 2) as avg_fare,
    ROUND(SUM(f.total_amount)::numeric, 2) as total_revenue
FROM fact_trips f
JOIN dim_payment p ON f.payment_key = p.payment_key
GROUP BY p.payment_name
ORDER BY trip_count DESC;

-- Query 5: Top pickup locations
SELECT
    l.location_id,
    l.borough,
    l.zone,
    COUNT(*) as trip_count
FROM fact_trips f
JOIN dim_location l ON f.pickup_location_key = l.location_key
GROUP BY l.location_id, l.borough, l.zone
ORDER BY trip_count DESC
LIMIT 20;

-- Query 6: Monthly revenue trends
SELECT
    d.year,
    d.month,
    COUNT(*) as trip_count,
    ROUND(SUM(f.total_amount)::numeric, 2) as total_revenue,
    ROUND(AVG(f.total_amount)::numeric, 2) as avg_fare_per_trip,
    ROUND(SUM(f.tip_amount)::numeric, 2) as total_tips
FROM fact_trips f
JOIN dim_datetime d ON f.datetime_key = d.datetime_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Query 7: Trip distance distribution
SELECT
    CASE
        WHEN trip_distance < 1 THEN '< 1 mile'
        WHEN trip_distance < 3 THEN '1-3 miles'
        WHEN trip_distance < 5 THEN '3-5 miles'
        WHEN trip_distance < 10 THEN '5-10 miles'
        WHEN trip_distance < 20 THEN '10-20 miles'
        ELSE '20+ miles'
    END as distance_range,
    COUNT(*) as trip_count,
    ROUND(AVG(total_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(trip_duration_minutes)::numeric, 2) as avg_duration
FROM fact_trips
GROUP BY distance_range
ORDER BY
    CASE
        WHEN trip_distance < 1 THEN 1
        WHEN trip_distance < 3 THEN 2
        WHEN trip_distance < 5 THEN 3
        WHEN trip_distance < 10 THEN 4
        WHEN trip_distance < 20 THEN 5
        ELSE 6
    END;

-- Query 8: Passenger count analysis
SELECT
    passenger_count,
    COUNT(*) as trip_count,
    ROUND(AVG(total_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance
FROM fact_trips
WHERE passenger_count > 0
GROUP BY passenger_count
ORDER BY passenger_count;

-- Query 9: Peak hours analysis
SELECT
    d.hour,
    d.is_weekend,
    COUNT(*) as trip_count,
    ROUND(AVG(f.total_amount)::numeric, 2) as avg_fare
FROM fact_trips f
JOIN dim_datetime d ON f.datetime_key = d.datetime_key
GROUP BY d.hour, d.is_weekend
ORDER BY trip_count DESC
LIMIT 20;

-- Query 10: Data quality checks
SELECT
    'Total trips' as metric,
    COUNT(*) as value
FROM fact_trips
UNION ALL
SELECT
    'Unique datetime keys',
    COUNT(DISTINCT datetime_key)
FROM fact_trips
UNION ALL
SELECT
    'Unique pickup locations',
    COUNT(DISTINCT pickup_location_key)
FROM fact_trips
UNION ALL
SELECT
    'Unique dropoff locations',
    COUNT(DISTINCT dropoff_location_key)
FROM fact_trips
UNION ALL
SELECT
    'Avg trip duration (minutes)',
    ROUND(AVG(trip_duration_minutes)::numeric, 2)
FROM fact_trips
UNION ALL
SELECT
    'Avg fare amount',
    ROUND(AVG(total_amount)::numeric, 2)
FROM fact_trips;
