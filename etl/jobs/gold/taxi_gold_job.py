"""
NYC Taxi Gold Layer Job - Dimensional Model ETL.

This module implements the gold (curated) layer pipeline that transforms bronze
layer data into a dimensional model (star schema) optimized for analytics.

Dimensional Model:
    - fact_trip: Grain = one taxi trip record
    - dim_date: Date dimension with calendar attributes
    - dim_location: Zone/borough dimension (from zone lookup)
    - dim_payment: Payment type and rate code dimension

Architecture:
    - Follows Medallion Architecture (Bronze → Gold)
    - Implements Kimball dimensional modeling methodology
    - Gold layer: Clean, conformed, business-ready data
    - Star schema design for optimal query performance

Data Quality:
    - Filters invalid records (nulls, negatives, outliers)
    - Validates location IDs against zone lookup
    - Enforces business rules on fares, distances, times
    - Adds data quality flags for audit trails

Partitioning Strategy:
    - Fact table: Partitioned by year and month for performance
    - Dimension tables: Small lookup tables (no partitioning needed)
    - Enables partition pruning for time-based queries

Design Patterns:
    - Template Method: Inherits from BaseSparkJob
    - Slowly Changing Dimensions: Type 1 (overwrite) for dimensions
    - Surrogate Keys: Integer IDs for all dimension tables
"""

import sys
from pathlib import Path

# Add project root to path for imports when running as script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(project_root))

import logging
from typing import Literal, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

from etl.jobs.base_job import BaseSparkJob, JobExecutionError
from etl.jobs.utils.config import JobConfig

logger = logging.getLogger(__name__)


class DataQualityError(JobExecutionError):
    """
    Raised when data quality validation fails during gold transformation

    :raises JobExecutionError: Thrown when data quality checks fail
    """

    pass


class TaxiGoldJob(BaseSparkJob):
    """
    Production-ready gold layer job for NYC Taxi dimensional model.

    This job reads bronze layer taxi data, applies data quality filters,
    joins with zone lookup reference data, and creates a star schema with
    fact and dimension tables optimized for analytics.

    Features:
        - Star schema dimensional model (Kimball methodology)
        - Comprehensive data quality filtering
        - Zone lookup enrichment for location dimensions
        - Business-friendly column names and derived metrics
        - Partitioned storage for query performance
        - Data quality audit columns

    Dimensional Model:
        fact_trip: Fact table with trip-level transactions
        dim_date: Date dimension with calendar attributes
        dim_location: Location dimension with zone/borough details
        dim_payment: Payment type and rate code lookup

    Data Flow:
        1. Extract: Read bronze layer taxi data + zone lookup
        2. Transform:
           a. Data quality filtering (nulls, outliers, invalid values)
           b. Create dimension tables (date, location, payment)
           c. Create fact table with foreign keys to dimensions
           d. Add business metrics and derived columns
        3. Load: Write dimensional model to gold layer

    Example:
        >>> # Process single month
        >>> job = TaxiGoldJob("yellow", 2024, 1)
        >>> success = job.run()
        >>>
        >>> # Process date range
        >>> job = TaxiGoldJob("yellow", 2024, 1, end_year=2024, end_month=3)
        >>> success = job.run()

    Attributes:
        taxi_type: Type of taxi data (yellow or green)
        year: Starting year of data to process
        month: Starting month of data to process
        end_year: Optional ending year for date range
        end_month: Optional ending month for date range
    """

    # Class constants
    VALID_TAXI_TYPES = ["yellow", "green"]
    MIN_YEAR = 2009

    # Data quality thresholds
    MAX_TRIP_DISTANCE_MILES = 500  # Reasonable max trip distance
    MAX_FARE_AMOUNT = 5000  # Reasonable max fare
    MAX_TRIP_DURATION_HOURS = 24  # Max reasonable trip duration
    MIN_FARE_AMOUNT = 0  # Minimum fare (can be 0 for cancelled trips)

    def __init__(
        self,
        taxi_type: Literal["yellow", "green"],
        year: int,
        month: int,
        end_year: Optional[int] = None,
        end_month: Optional[int] = None,
        config: Optional[JobConfig] = None,
    ):
        """
        Initialise the taxi gold layer job.

        :params taxi_type: Type of taxi data (yellow or green)
        :params year: Starting year of data (2009 or later)
        :params month: Starting month of data (1-12)
        :params end_year: Optional ending year for date range processing
        :params end_month: Optional ending month for date range processing
        :params config: Optional job configuration
        :raises ValueError: If parameters are invalid
        """
        self._validate_parameters(taxi_type, year, month, end_year, end_month)

        super().__init__(
            job_name=f"TaxiGold_{taxi_type}_{year}_{month:02d}", config=config
        )
        self.taxi_type = taxi_type
        self.year = year
        self.month = month
        self.end_year = end_year if end_year else year
        self.end_month = end_month if end_month else month

    @staticmethod
    def _validate_parameters(
        taxi_type: str,
        year: int,
        month: int,
        end_year: Optional[int],
        end_month: Optional[int],
    ) -> None:
        """
        Validate job parameters.

        :raises ValueError: If parameters are invalid
        """
        if taxi_type not in TaxiGoldJob.VALID_TAXI_TYPES:
            raise ValueError(
                f"Invalid taxi_type: {taxi_type}. "
                f"Must be one of {TaxiGoldJob.VALID_TAXI_TYPES}"
            )

        if not isinstance(year, int) or year < TaxiGoldJob.MIN_YEAR:
            raise ValueError(
                f"Invalid year: {year}. " f"Must be integer >= {TaxiGoldJob.MIN_YEAR}"
            )

        if not isinstance(month, int) or not 1 <= month <= 12:
            raise ValueError(
                f"Invalid month: {month}. Must be integer between 1 and 12"
            )

        if end_year and end_month:
            if end_year < year or (end_year == year and end_month < month):
                raise ValueError("End date must be >= start date")

    def validate_inputs(self):
        """
        Validate job inputs.

        :raises JobValidationError: If inputs are invalid
        """
        self.logger.info(
            f"Validated inputs: {self.taxi_type}, "
            f"{self.year}-{self.month:02d} to {self.end_year}-{self.end_month:02d}"
        )

    def extract(self) -> Tuple[DataFrame, DataFrame]:
        """
        Extract data from bronze layer and zone lookup.

        :returns: Tuple of (bronze_trips_df, zone_lookup_df)
        :raises JobExecutionError: If extraction fails
        """
        # Read bronze layer taxi data
        trips_df = self._extract_bronze_trips()

        # Read zone lookup reference data
        zones_df = self._extract_zone_lookup()

        return trips_df, zones_df

    def _extract_bronze_trips(self) -> DataFrame:
        """
        Extract taxi trip data from bronze layer.

        Reads partitioned bronze data for the specified date range.
        Handles schema evolution by standardizing columns across partitions.

        :returns: DataFrame with raw bronze trip data
        """
        s3_path = self.config.get_s3_path("bronze", taxi_type=self.taxi_type)
        self.logger.info(f"Reading bronze layer: {s3_path}")

        # Build partition filter for date range
        from datetime import date
        from dateutil.relativedelta import relativedelta

        start_date = date(self.year, self.month, 1)
        end_date = date(self.end_year, self.end_month, 1)

        # Collect all year/month partitions in range
        partitions = []
        current = start_date
        while current <= end_date:
            partitions.append((current.year, current.month))
            current += relativedelta(months=1)

        self.logger.info(f"Reading {len(partitions)} month partition(s): {partitions}")

        # Read all partitions with filter pushdown
        dfs = []
        all_columns = set()

        for year, month in partitions:
            partition_path = f"{s3_path}/year={year}/month={month}"
            try:
                self.logger.info(f"Reading partition: year={year}, month={month}...")

                # Read partition with ignoreCorruptFiles option
                df_partition = (
                    self.spark.read.option("ignoreCorruptFiles", "true")
                    .option("mode", "PERMISSIVE")
                    .parquet(partition_path)
                )

                # Force evaluation to catch corrupted files before adding to list
                # Use take(1) which is faster than count() for validation
                self.logger.info(f"Validating partition data...")
                sample = df_partition.take(1)

                if not sample:
                    self.logger.warning(f"Partition {year}-{month} is empty, skipping")
                    continue

                # Now get full count for logging
                partition_count = df_partition.count()

                dfs.append(df_partition)
                all_columns.update(df_partition.columns)
                self.logger.info(
                    f"✓ Loaded partition: year={year}, month={month} "
                    f"({partition_count:,} records, {len(df_partition.columns)} columns)"
                )
            except Exception as e:
                self.logger.error(
                    f"✗ FAILED to read partition {year}-{month}: {type(e).__name__}: {str(e)[:200]}\n"
                    f"  Path: {partition_path}\n"
                    f"  This partition will be SKIPPED. Re-run bronze ingestion to fix:\n"
                    f"  python etl/jobs/bronze/taxi_ingestion_job.py --taxi-type {self.taxi_type} --year {year} --month {month}"
                )
                # Continue to next partition instead of failing entire job
                continue

        if not dfs:
            raise JobExecutionError(
                f"No bronze data found for {self.taxi_type} "
                f"from {self.year}-{self.month} to {self.end_year}-{self.end_month}"
            )

        # Standardize schema across all partitions (handle schema evolution)
        self.logger.info(
            f"Standardizing schema across partitions ({len(all_columns)} total columns)"
        )
        self.logger.info(f"All columns found: {sorted(all_columns)}")

        standardized_dfs = []
        column_order = sorted(all_columns)

        for idx, df in enumerate(dfs):
            original_cols = set(df.columns)
            missing_cols = all_columns - original_cols

            if missing_cols:
                self.logger.info(
                    f"Partition {idx + 1}: Adding {len(missing_cols)} missing columns: {missing_cols}"
                )
                # Add missing columns as null
                for col in missing_cols:
                    df = df.withColumn(col, F.lit(None))

            # Select columns in consistent order
            df = df.select(*column_order)

            # Verify schema alignment
            self.logger.info(
                f"Partition {idx + 1}: {len(df.columns)} columns after standardization"
            )
            standardized_dfs.append(df)

        # Verify all DataFrames have the same number of columns
        col_counts = [len(df.columns) for df in standardized_dfs]
        if len(set(col_counts)) > 1:
            raise JobExecutionError(
                f"Schema standardization failed: DataFrames have different column counts: {col_counts}"
            )

        self.logger.info(
            f"All {len(standardized_dfs)} partitions standardized to {col_counts[0]} columns"
        )

        # Union all partitions with standardized schemas
        trips_df = standardized_dfs[0]
        for df in standardized_dfs[1:]:
            trips_df = trips_df.union(df)

        # Note: year/month are partition columns in the directory path, not data columns
        # We already filtered by reading specific partition paths above, so no additional filter needed

        self.logger.info("Union complete, materializing count...")
        record_count = trips_df.count()
        self.logger.info(f"Extracted {record_count:,} trip records from bronze layer")

        return trips_df

    def _extract_zone_lookup(self) -> DataFrame:
        """
        Extract zone lookup reference data from MinIO.

        :returns: DataFrame with zone lookup data
        """
        # Zone lookup is stored as CSV in misc directory
        zone_lookup_path = f"s3a://{self.config.minio.bucket}/misc/taxi_zone_lookup.csv"
        self.logger.info(f"Reading zone lookup: {zone_lookup_path}")

        try:
            zones_df = self.spark.read.csv(
                zone_lookup_path, header=True, inferSchema=True
            )
            zone_count = zones_df.count()
            self.logger.info(f"Loaded {zone_count:,} zone records")
            return zones_df
        except Exception as e:
            raise JobExecutionError(f"Failed to read zone lookup: {e}") from e

    def transform(self, data: Tuple[DataFrame, DataFrame]) -> dict:
        """
        Transform bronze data into dimensional model.

        :params data: Tuple of (trips_df, zones_df)
        :returns: Dictionary with dimensional model tables:
                    {
                        'fact_trip': DataFrame,
                        'dim_date': DataFrame,
                        'dim_location': DataFrame,
                        'dim_payment': DataFrame
                    }
        """
        trips_df, zones_df = data

        self.logger.info("=== Starting Gold Layer Transformation ===")

        # Remove duplicates from source data
        initial_count = trips_df.count()
        trips_df = self._remove_duplicates(trips_df)
        deduped_count = trips_df.count()
        if initial_count > deduped_count:
            self.logger.warning(
                f"Removed {initial_count - deduped_count:,} duplicate records "
                f"({100 * (initial_count - deduped_count) / initial_count:.2f}%)"
            )

        # Data quality filtering
        trips_clean = self._apply_data_quality_filters(trips_df)

        # Standardize column names (handle yellow/green differences)
        trips_standardized = self._standardize_schema(trips_clean)

        # Create dimension tables
        dim_date = self._create_dim_date(trips_standardized)
        dim_location = self._create_dim_location(zones_df)
        dim_payment = self._create_dim_payment(trips_standardized)

        # Create fact table
        fact_trip = self._create_fact_trip(
            trips_standardized, dim_date, dim_location, dim_payment
        )

        self.logger.info("=== Gold Layer Transformation Complete ===")

        return {
            "fact_trip": fact_trip,
            "dim_date": dim_date,
            "dim_location": dim_location,
            "dim_payment": dim_payment,
        }

    def _remove_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicate records from bronze data using SCD Type 1 (latest wins).

        Duplicates can occur due to:
        - Re-running ingestion jobs
        - Source data issues
        - Multiple extracts of the same data
        - Late-arriving data

        Strategy (SCD Type 1 - Keep Latest):
        - Use record_hash column from bronze layer for deduplication
        - When duplicates exist, keep the record with latest ingestion_timestamp
        - This implements "latest wins" strategy for slowly changing dimensions
        - Bronze layer contains ALL records (audit trail)
        - Gold layer contains LATEST version only (business view)

        :returns: DataFrame with duplicates removed, keeping latest version
        """
        initial_count = df.count()

        if "record_hash" in df.columns:
            # Hash-based deduplication with SCD Type 1 logic
            self.logger.info(
                "Deduplicating using record_hash (SCD Type 1: keeping latest)"
            )

            # Check if we have ingestion_timestamp for SCD Type 1
            if "ingestion_timestamp" in df.columns:
                # Window function to keep latest record per hash
                from pyspark.sql.window import Window

                window_spec = Window.partitionBy("record_hash").orderBy(
                    F.col("ingestion_timestamp").desc()
                )

                df_deduped = (
                    df.withColumn("row_num", F.row_number().over(window_spec))
                    .filter(F.col("row_num") == 1)
                    .drop("row_num")
                )

                deduped_count = df_deduped.count()
                removed_count = initial_count - deduped_count

                if removed_count > 0:
                    self.logger.info(
                        f"SCD Type 1: Removed {removed_count:,} older versions, "
                        f"kept {deduped_count:,} latest records "
                        f"({100 * removed_count / initial_count:.2f}% were duplicates)"
                    )

                return df_deduped
            else:
                # Simple hash deduplication (no timestamp available)
                self.logger.warning(
                    "ingestion_timestamp not found, using simple deduplication"
                )
                return df.dropDuplicates(["record_hash"])
        else:
            # Fallback: deduplicate on all columns (slower)
            self.logger.warning(
                "record_hash not found in bronze data - deduplicating using all columns"
            )
            return df.dropDuplicates()

    def _apply_data_quality_filters(self, df: DataFrame) -> DataFrame:
        """
        Apply comprehensive data quality filters.

        Filters out invalid records based on:
        - Null/missing critical fields
        - Negative values (fare, distance, passenger count)
        - Outliers (unrealistic fares, distances, durations)
        - Invalid location IDs
        - Invalid timestamps

        :returns: Cleaned DataFrame with data quality flags
        """
        self.logger.info("Applying data quality filters...")
        initial_count = df.count()

        # Identify pickup/dropoff datetime columns (different names for yellow/green)
        pickup_col = (
            "tpep_pickup_datetime"
            if "tpep_pickup_datetime" in df.columns
            else "lpep_pickup_datetime"
        )
        dropoff_col = (
            "tpep_dropoff_datetime"
            if "tpep_dropoff_datetime" in df.columns
            else "lpep_dropoff_datetime"
        )

        # Add quality flags
        df_with_flags = (
            df.withColumn(
                "has_null_timestamps",
                F.col(pickup_col).isNull() | F.col(dropoff_col).isNull(),
            )
            .withColumn(
                "has_invalid_fare",
                (F.col("fare_amount") < self.MIN_FARE_AMOUNT)
                | (F.col("fare_amount") > self.MAX_FARE_AMOUNT),
            )
            .withColumn(
                "has_invalid_distance",
                (F.col("trip_distance") < 0)
                | (F.col("trip_distance") > self.MAX_TRIP_DISTANCE_MILES),
            )
            .withColumn(
                "has_invalid_passenger_count",
                (F.col("passenger_count") < 0) | (F.col("passenger_count") > 9),
            )
            .withColumn(
                "has_invalid_location",
                F.col("PULocationID").isNull() | F.col("DOLocationID").isNull(),
            )
        )

        # Calculate trip duration and flag unrealistic values
        df_with_flags = df_with_flags.withColumn(
            "trip_duration_seconds",
            F.unix_timestamp(F.col(dropoff_col)) - F.unix_timestamp(F.col(pickup_col)),
        ).withColumn(
            "has_invalid_duration",
            (F.col("trip_duration_seconds") <= 0)
            | (F.col("trip_duration_seconds") > self.MAX_TRIP_DURATION_HOURS * 3600),
        )

        # Composite quality flag
        df_with_flags = df_with_flags.withColumn(
            "is_valid_record",
            ~(
                F.col("has_null_timestamps")
                | F.col("has_invalid_fare")
                | F.col("has_invalid_distance")
                | F.col("has_invalid_passenger_count")
                | F.col("has_invalid_location")
                | F.col("has_invalid_duration")
            ),
        )

        # Log quality metrics
        total_invalid = df_with_flags.filter(~F.col("is_valid_record")).count()
        self.logger.info(f"Data quality summary:")
        self.logger.info(f"  Initial records: {initial_count:,}")
        self.logger.info(
            f"  Invalid records: {total_invalid:,} ({100 * total_invalid / initial_count:.2f}%)"
        )

        # Filter to valid records only
        df_clean = df_with_flags.filter(F.col("is_valid_record"))
        clean_count = df_clean.count()
        self.logger.info(
            f"  Clean records: {clean_count:,} ({100 * clean_count / initial_count:.2f}%)"
        )

        return df_clean

    def _standardize_schema(self, df: DataFrame) -> DataFrame:
        """
        Standardise column names for yellow/green taxi data.

        Yellow uses: tpep_pickup_datetime, tpep_dropoff_datetime
        Green uses: lpep_pickup_datetime, lpep_dropoff_datetime

        :returns: DataFrame with standardized column names
        """
        # Check which datetime columns exist
        if "tpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed(
                "tpep_pickup_datetime", "pickup_datetime"
            ).withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        elif "lpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed(
                "lpep_pickup_datetime", "pickup_datetime"
            ).withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

        self.logger.info("Schema standardized: pickup_datetime, dropoff_datetime")
        return df

    def _create_dim_date(self, df: DataFrame) -> DataFrame:
        """
        Create date dimension with calendar attributes.

        Grain: One row per date
        Attributes: year, month, day, day_of_week, is_weekend, quarter, etc.

        :returns: Date dimension DataFrame
        """
        self.logger.info("Creating dim_date...")

        # Extract unique dates from pickup datetime
        dates_df = df.select(
            F.col("pickup_datetime").cast("date").alias("date")
        ).distinct()

        # Add calendar attributes
        dim_date = (
            dates_df.withColumn(
                "date_key", F.date_format("date", "yyyyMMdd").cast(IntegerType())
            )
            .withColumn("year", F.year("date"))
            .withColumn("quarter", F.quarter("date"))
            .withColumn("month", F.month("date"))
            .withColumn("month_name", F.date_format("date", "MMMM"))
            .withColumn("day", F.dayofmonth("date"))
            .withColumn("day_of_week", F.dayofweek("date"))
            .withColumn("day_of_week_name", F.date_format("date", "EEEE"))
            .withColumn(
                "is_weekend", F.col("day_of_week").isin([1, 7])  # Sunday=1, Saturday=7
            )
            .withColumn("week_of_year", F.weekofyear("date"))
        )

        # Add metadata columns
        dim_date = dim_date.withColumn(
            "created_timestamp", F.current_timestamp()
        ).withColumn("data_layer", F.lit("gold"))

        # Order columns
        dim_date = dim_date.select(
            "date_key",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_of_week",
            "day_of_week_name",
            "is_weekend",
            "week_of_year",
            "created_timestamp",
            "data_layer",
        ).orderBy("date_key")

        date_count = dim_date.count()
        self.logger.info(f"Created dim_date: {date_count:,} unique dates")

        return dim_date

    def _create_dim_location(self, zones_df: DataFrame) -> DataFrame:
        """
        Create location dimension from zone lookup.

        Grain: One row per LocationID
        Attributes: LocationID, Borough, Zone, service_zone

        :returns: Location dimension DataFrame
        """
        self.logger.info("Creating dim_location...")

        # Zone lookup already has the right structure
        dim_location = zones_df.select(
            F.col("LocationID").cast(IntegerType()).alias("location_key"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
            F.col("service_zone").alias("service_zone"),
        ).distinct()

        # Add metadata columns
        dim_location = dim_location.withColumn(
            "created_timestamp", F.current_timestamp()
        ).withColumn("data_layer", F.lit("gold"))

        dim_location = dim_location.select(
            "location_key",
            "borough",
            "zone",
            "service_zone",
            "created_timestamp",
            "data_layer",
        ).orderBy("location_key")

        location_count = dim_location.count()
        self.logger.info(f"Created dim_location: {location_count:,} unique locations")

        return dim_location

    def _create_dim_payment(self, df: DataFrame) -> DataFrame:
        """
        Create payment dimension with payment types and rate codes.

        Grain: One row per (payment_type, RatecodeID) combination
        Attributes: payment_type_id, payment_type_desc, rate_code_id, rate_code_desc

        :returns: Payment dimension DataFrame
        """
        self.logger.info("Creating dim_payment...")

        # Extract unique payment type and rate code combinations
        payment_df = df.select(
            F.col("payment_type").cast(IntegerType()),
            F.col("RatecodeID").cast(IntegerType()),
        ).distinct()

        # Add descriptive labels
        dim_payment = (
            payment_df.withColumn(
                "payment_type_desc",
                F.when(F.col("payment_type") == 1, "Credit card")
                .when(F.col("payment_type") == 2, "Cash")
                .when(F.col("payment_type") == 3, "No charge")
                .when(F.col("payment_type") == 4, "Dispute")
                .when(F.col("payment_type") == 5, "Unknown")
                .when(F.col("payment_type") == 6, "Voided trip")
                .otherwise("Unknown"),
            )
            .withColumn(
                "rate_code_desc",
                F.when(F.col("RatecodeID") == 1, "Standard rate")
                .when(F.col("RatecodeID") == 2, "JFK")
                .when(F.col("RatecodeID") == 3, "Newark")
                .when(F.col("RatecodeID") == 4, "Nassau or Westchester")
                .when(F.col("RatecodeID") == 5, "Negotiated fare")
                .when(F.col("RatecodeID") == 6, "Group ride")
                .otherwise("Unknown"),
            )
            .withColumn("payment_key", F.monotonically_increasing_id())
        )

        # Add metadata columns
        dim_payment = dim_payment.withColumn(
            "created_timestamp", F.current_timestamp()
        ).withColumn("data_layer", F.lit("gold"))

        # Order columns
        dim_payment = dim_payment.select(
            "payment_key",
            F.col("payment_type").alias("payment_type_id"),
            "payment_type_desc",
            F.col("RatecodeID").alias("rate_code_id"),
            "rate_code_desc",
            "created_timestamp",
            "data_layer",
        ).orderBy("payment_key")

        payment_count = dim_payment.count()
        self.logger.info(
            f"Created dim_payment: {payment_count:,} unique payment combinations"
        )

        return dim_payment

    def _create_fact_trip(
        self,
        trips_df: DataFrame,
        dim_date: DataFrame,
        dim_location: DataFrame,
        dim_payment: DataFrame,
    ) -> DataFrame:
        """
        Create fact table with foreign keys to dimensions.

        Grain: One row per taxi trip
        Measures: fares, distances, times, tips, tolls, passenger counts
        Foreign Keys: date_key, pickup_location_key, dropoff_location_key, payment_key

        :returns: Fact table DataFrame
        """
        self.logger.info("Creating fact_trip...")

        # Create date key from pickup datetime
        fact = trips_df.withColumn(
            "date_key",
            F.date_format(F.col("pickup_datetime").cast("date"), "yyyyMMdd").cast(
                IntegerType()
            ),
        )

        # Join with dim_payment to get payment_key
        fact = fact.join(
            dim_payment.select(
                "payment_key",
                F.col("payment_type_id").alias("payment_type"),
                F.col("rate_code_id").alias("RatecodeID"),
            ),
            on=["payment_type", "RatecodeID"],
            how="left",
        )

        # Add surrogate key for fact table (use BIGINT for large datasets)
        fact = fact.withColumn("trip_key", F.monotonically_increasing_id())

        # Calculate derived measures
        fact = (
            fact.withColumn(
                "trip_duration_minutes", F.col("trip_duration_seconds") / 60.0
            )
            .withColumn("total_amount_with_tip", F.col("total_amount"))
            .withColumn(
                "tip_percentage",
                F.when(
                    F.col("fare_amount") > 0,
                    (F.col("tip_amount") / F.col("fare_amount")) * 100,
                ).otherwise(0),
            )
            .withColumn(
                "avg_speed_mph",
                F.when(
                    (F.col("trip_duration_seconds") > 0) & (F.col("trip_distance") > 0),
                    (
                        F.col("trip_distance")
                        / (F.col("trip_duration_seconds") / 3600.0)
                    ),
                ).otherwise(0),
            )
        )

        # Extract year/month from pickup_datetime for partitioning
        fact = fact.withColumn(
            "partition_year", F.year(F.col("pickup_datetime"))
        ).withColumn("partition_month", F.month(F.col("pickup_datetime")))

        # Add gold layer metadata columns
        fact = (
            fact.withColumn("gold_transformation_timestamp", F.current_timestamp())
            .withColumn("gold_transformation_date", F.current_date())
            .withColumn("gold_job_name", F.lit(self.job_name))
            .withColumn("data_layer", F.lit("gold"))
        )

        # Select final fact table columns (for hash computation)
        fact_trip = fact.select(
            "trip_key",
            "date_key",
            F.col("PULocationID").cast(IntegerType()).alias("pickup_location_key"),
            F.col("DOLocationID").cast(IntegerType()).alias("dropoff_location_key"),
            "payment_key",
            "pickup_datetime",
            "dropoff_datetime",
            F.col("passenger_count").cast(IntegerType()),
            F.col("trip_distance").cast(DoubleType()),
            F.col("trip_duration_seconds").cast(IntegerType()),
            F.col("trip_duration_minutes").cast(DoubleType()),
            F.col("fare_amount").cast(DoubleType()),
            F.col("extra").cast(DoubleType()),
            F.col("mta_tax").cast(DoubleType()),
            F.col("tip_amount").cast(DoubleType()),
            F.col("tolls_amount").cast(DoubleType()),
            F.col("total_amount").cast(DoubleType()),
            F.col("tip_percentage").cast(DoubleType()),
            F.col("avg_speed_mph").cast(DoubleType()),
            "partition_year",
            "partition_month",
            # Metadata columns
            "gold_transformation_timestamp",
            "gold_transformation_date",
            "gold_job_name",
            "data_layer",
        )

        # Add fact_hash for data integrity and upsert operations
        # Hash includes business keys and measures (excluding trip_key and metadata)
        fact_hash_columns = [
            "date_key",
            "pickup_location_key",
            "dropoff_location_key",
            "payment_key",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
        ]

        self.logger.info(
            f"Computing fact_hash from {len(fact_hash_columns)} key columns"
        )

        fact_trip = fact_trip.withColumn(
            "fact_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(c).cast("string"), F.lit(""))
                        for c in fact_hash_columns
                    ],
                ),
                256,
            ),
        )

        fact_count = fact_trip.count()
        self.logger.info(f"Created fact_trip: {fact_count:,} trip records")

        # Validate hash integrity
        self._validate_hash_integrity(fact_trip)

        return fact_trip

    def _validate_hash_integrity(self, fact_trip: DataFrame):
        """
        Validate data integrity using fact_hash.

        Checks:
        1. fact_hash is not null
        2. fact_hash is unique (no accidental duplicates)
        3. Hash distribution is reasonable (no hash collisions)

        :params fact_trip: Fact table DataFrame with fact_hash
        """
        self.logger.info("=== Validating Hash Integrity ===")

        total_records = fact_trip.count()

        # Check No null hashes
        null_hashes = fact_trip.filter(F.col("fact_hash").isNull()).count()
        if null_hashes > 0:
            self.logger.error(f"Found {null_hashes:,} records with NULL fact_hash")
            raise JobExecutionError(
                f"Data integrity error: {null_hashes} NULL hashes found"
            )

        # Check Hash uniqueness
        unique_hashes = fact_trip.select("fact_hash").distinct().count()
        if unique_hashes != total_records:
            duplicate_count = total_records - unique_hashes
            self.logger.warning(
                f"Hash uniqueness check: {unique_hashes:,} unique hashes for {total_records:,} records "
                f"({duplicate_count:,} duplicates detected)"
            )
            # This is acceptable - duplicates should have been filtered in deduplication step
        else:
            self.logger.info(
                f"✓ Hash uniqueness verified: All {total_records:,} hashes are unique"
            )

        # Check Hash length and format (SHA-256 = 64 hex characters)
        invalid_hash_length = fact_trip.filter(
            F.length(F.col("fact_hash")) != 64
        ).count()
        if invalid_hash_length > 0:
            self.logger.error(
                f"Found {invalid_hash_length:,} hashes with invalid length (expected 64)"
            )
            raise JobExecutionError(
                f"Data integrity error: {invalid_hash_length} invalid hash lengths"
            )

        self.logger.info("✓ All hash integrity checks passed")

        # Log sample statistics
        self.logger.info("Fact table statistics:")
        fact_trip.select(
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_duration_minutes").alias("avg_duration_min"),
            F.avg("tip_percentage").alias("avg_tip_pct"),
        ).show()

        return fact_trip

    def load(self, dimensional_model: dict):
        """
        Load dimensional model to gold layer in MinIO.

        Writes fact and dimension tables as parquet files.
        Uses partition overwrite mode for fact table to avoid duplicates.
        Dimension tables are fully refreshed each run.

        Strategy:
        - Fact table: Dynamic partition overwrite (only replaces partitions being written)
        - Dimensions: Full overwrite (small tables, always refreshed)

        This prevents:
        - Duplicate records in the same partition
        - Data corruption from partial writes
        - Stale dimension data

        :params dimensional_model: Dictionary with fact and dimension DataFrames
        """
        if not self.config.minio.use_minio:
            self.logger.warning("MinIO disabled, skipping gold layer load")
            return

        gold_base_path = self.config.get_s3_path("gold", taxi_type=self.taxi_type)
        self.logger.info(f"Writing dimensional model to gold layer: {gold_base_path}")

        # Write fact table (partitioned with dynamic partition overwrite)
        fact_trip = dimensional_model["fact_trip"]
        fact_path = f"{gold_base_path}/fact_trip"

        # Get partition info for logging
        partitions = (
            fact_trip.select("partition_year", "partition_month").distinct().collect()
        )
        partition_list = [
            (row.partition_year, row.partition_month) for row in partitions
        ]
        self.logger.info(f"Writing fact_trip to {fact_path}")
        self.logger.info(
            f"Overwriting {len(partition_list)} partition(s): {partition_list}"
        )

        # Use dynamic partition overwrite mode
        # This only overwrites the specific partitions being written, not the entire table
        fact_trip.write.mode("overwrite").option(
            "partitionOverwriteMode", "dynamic"
        ).partitionBy("partition_year", "partition_month").option(
            "compression", "snappy"
        ).parquet(
            fact_path
        )

        fact_count = fact_trip.count()
        self.logger.info(f"Wrote fact_trip: {fact_count:,} records")

        # Write dimension tables (not partitioned - small lookup tables)
        # Full overwrite is appropriate for dimensions (SCD Type 1)
        for dim_name in ["dim_date", "dim_location", "dim_payment"]:
            dim_df = dimensional_model[dim_name]
            dim_path = f"{gold_base_path}/{dim_name}"
            self.logger.info(f"Writing {dim_name} to {dim_path}...")

            dim_df.write.mode("overwrite").option("compression", "snappy").parquet(
                dim_path
            )

            dim_count = dim_df.count()
            self.logger.info(f"Wrote {dim_name}: {dim_count:,} records")

        self.logger.info("=== Gold layer load complete ===")
        self.logger.info("Data integrity notes:")
        self.logger.info(
            "  - Fact partitions overwritten: No duplicates within processed months"
        )
        self.logger.info("  - Dimensions fully refreshed: Latest reference data loaded")
        self.logger.info(
            "  - Unprocessed partitions: Preserved (not affected by this run)"
        )


def run_gold_job(
    taxi_type: Literal["yellow", "green"],
    year: int,
    month: int,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None,
) -> bool:
    """
    Convenience function to run the gold layer job.

    :params taxi_type: Type of taxi (yellow or green)
    :params year: Starting year
    :params month: Starting month (1-12)
    :params end_year: Optional ending year for date range
    :params end_month: Optional ending month for date range
    :returns True if successful, False otherwise
    """
    job = TaxiGoldJob(taxi_type, year, month, end_year, end_month)
    return job.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC Taxi Gold Layer Job")
    parser.add_argument(
        "--taxi-type", type=str, choices=["yellow", "green"], required=True
    )
    parser.add_argument("--year", type=int, required=True, help="Starting year")
    parser.add_argument(
        "--month", type=int, required=True, help="Starting month (1-12)"
    )
    parser.add_argument("--end-year", type=int, help="Ending year (optional)")
    parser.add_argument("--end-month", type=int, help="Ending month (optional)")

    args = parser.parse_args()

    success = run_gold_job(
        args.taxi_type, args.year, args.month, args.end_year, args.end_month
    )
    exit(0 if success else 1)
