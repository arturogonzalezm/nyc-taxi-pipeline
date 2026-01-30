# Dataset Explanation

## Overview

This pipeline processes **NYC Taxi & Limousine Commission (TLC) Trip Record Data**, one of the most widely used public datasets for data engineering and analytics projects.

## Data Source

| Attribute | Value |
|-----------|-------|
| **Provider** | NYC Taxi & Limousine Commission |
| **URL** | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| **Format** | Apache Parquet |
| **Update Frequency** | Monthly |
| **Historical Data** | Available from 2009 to present |

## Taxi Types

### Yellow Taxi

- **Description**: Traditional NYC yellow medallion taxis
- **Coverage**: Primarily Manhattan, airports, and major destinations
- **Hail Type**: Street hail and taxi stands
- **Data Available**: 2009 - present

### Green Taxi (Boro Taxi)

- **Description**: Street-hail livery vehicles (introduced 2013)
- **Coverage**: Outer boroughs (Brooklyn, Queens, Bronx, Staten Island) and upper Manhattan
- **Hail Type**: Street hail in designated areas
- **Data Available**: 2013 - present

## Dataset Fields

### Yellow Taxi Fields

| Field | Type | Description |
|-------|------|-------------|
| `VendorID` | Integer | TPEP provider (1=CMT, 2=VeriFone) |
| `tpep_pickup_datetime` | Timestamp | Pickup date and time |
| `tpep_dropoff_datetime` | Timestamp | Dropoff date and time |
| `passenger_count` | Integer | Number of passengers |
| `trip_distance` | Double | Trip distance in miles |
| `RatecodeID` | Integer | Rate code (1=Standard, 2=JFK, etc.) |
| `store_and_fwd_flag` | String | Store and forward flag (Y/N) |
| `PULocationID` | Integer | Pickup location zone ID |
| `DOLocationID` | Integer | Dropoff location zone ID |
| `payment_type` | Integer | Payment method (1=Credit, 2=Cash, etc.) |
| `fare_amount` | Double | Base fare amount |
| `extra` | Double | Extra charges |
| `mta_tax` | Double | MTA tax ($0.50) |
| `tip_amount` | Double | Tip amount (credit card only) |
| `tolls_amount` | Double | Toll charges |
| `improvement_surcharge` | Double | Improvement surcharge |
| `total_amount` | Double | Total trip cost |
| `congestion_surcharge` | Double | Congestion surcharge (since 2019) |
| `airport_fee` | Double | Airport fee (since 2022) |

### Green Taxi Fields

Similar to yellow taxi with these differences:

| Field | Type | Description |
|-------|------|-------------|
| `lpep_pickup_datetime` | Timestamp | Pickup date and time |
| `lpep_dropoff_datetime` | Timestamp | Dropoff date and time |
| `trip_type` | Integer | Trip type (1=Street-hail, 2=Dispatch) |

## Zone Lookup Reference

The pipeline also ingests the **Taxi Zone Lookup** reference data:

| Field | Type | Description |
|-------|------|-------------|
| `LocationID` | Integer | Unique zone identifier (1-265) |
| `Borough` | String | NYC borough name |
| `Zone` | String | Zone name/neighborhood |
| `service_zone` | String | Service zone category |

### Boroughs

- Manhattan
- Brooklyn
- Queens
- Bronx
- Staten Island
- EWR (Newark Airport)

## Data Volume

| Metric | Approximate Value |
|--------|-------------------|
| **Monthly Records** | 10-15 million (yellow), 1-2 million (green) |
| **Monthly File Size** | 200-400 MB (parquet) |
| **Annual Records** | 150-200 million combined |
| **Total Historical** | 2+ billion records |

## Data Quality Considerations

### Common Issues

1. **Null Values**: Some fields may be null (e.g., `passenger_count`, `RatecodeID`)
2. **Invalid Coordinates**: Pre-2017 data had lat/long; now uses zone IDs
3. **Outliers**: Extreme values in `trip_distance`, `fare_amount`, `tip_amount`
4. **Future Dates**: Occasional records with incorrect timestamps

### Pipeline Handling

The pipeline addresses these issues through:

- **Record Hashing**: Unique hash for deduplication
- **Null Handling**: Default values and null-safe operations
- **Range Validation**: Filters for reasonable values
- **Timestamp Validation**: Filters records outside expected ranges

## Usage Rights

The NYC TLC trip data is **public domain** and freely available for:

- Academic research
- Commercial applications
- Personal projects
- Data engineering practice

No API key or authentication required.
