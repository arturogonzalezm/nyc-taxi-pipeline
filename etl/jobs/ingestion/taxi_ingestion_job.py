"""
NYC Taxi Data Ingestion Job - Bronze Layer ETL.

This module implements the ingestion pipeline for NYC Taxi trip data, downloading
monthly parquet files from NYC TLC and storing them in the MinIO bronze layer with
appropriate partitioning for Delta Lake and CDC operations.

Architecture:
    - Follows Medallion Architecture (Bronze/Silver/Gold layers)
    - Bronze layer: Raw data with minimal transformation, metadata added
    - Implements caching strategy: MinIO cache with local fallback
    - Supports both single-month and bulk historical ingestion

Data Quality:
    - Validates data matches requested year/month
    - Enforces schema types for critical columns
    - Adds metadata for lineage tracking and CDC
    - Logs data distribution and match percentages

Partitioning Strategy:
    - Partitioned by year and month for query optimization
    - Supports Delta Lake operations
    - Enables efficient CDC (Change Data Capture)
    - Allows partition pruning for performance

Design Patterns:
    - Template Method: Inherits from BaseSparkJob
    - Strategy: Configurable extract (MinIO cache vs source)
    - Factory: Convenience functions for job creation
"""
import os
import requests
import logging
from pathlib import Path
from typing import Literal, Optional
from pyspark.sql import DataFrame
from minio import Minio
from minio.error import S3Error

from ..base_job import BaseSparkJob, JobExecutionError
from ..utils.config import JobConfig


logger = logging.getLogger(__name__)


class DataValidationError(JobExecutionError):
    """Raised when data validation fails during ingestion"""
    pass


class DownloadError(JobExecutionError):
    """Raised when data download from NYC TLC fails"""
    pass


class TaxiIngestionJob(BaseSparkJob):
    """
    Production-ready ingestion job for NYC Taxi trip data.

    This job downloads monthly taxi trip data from NYC TLC, validates it, adds
    metadata columns for lineage tracking, and loads it to the MinIO bronze layer
    with year/month partitioning.

    Features:
        - MinIO-first caching with local fallback
        - Data quality validation (year/month matching)
        - Schema enforcement for critical columns
        - Metadata addition for CDC and lineage tracking
        - Partitioned storage for Delta Lake compatibility
        - Comprehensive error handling and logging

    Data Flow:
        Extract -> Transform -> Load
        1. Extract: Download from NYC TLC (or use MinIO/local cache)
        2. Transform: Validate, enforce schema, add metadata (no business logic)
        3. Load: Write to MinIO bronze layer with partitioning

    Example:
        >>> # Single month ingestion
        >>> job = TaxiIngestionJob("yellow", 2024, 1)
        >>> success = job.run()
        >>>
        >>> # Bulk historical ingestion
        >>> results = run_bulk_ingestion("yellow", 2024, 1, 2024, 12)

    Attributes:
        taxi_type: Type of taxi data (yellow or green)
        year: Year of data to ingest
        month: Month of data to ingest (1-12)
        file_name: Generated filename for the parquet file
    """

    # Class constants
    NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    VALID_TAXI_TYPES = ["yellow", "green"]
    MIN_YEAR = 2009  # NYC TLC data starts from 2009
    MIN_MATCH_PERCENTAGE = 50.0  # Warn if less than 50% of records match requested period

    def __init__(
        self,
        taxi_type: Literal["yellow", "green"],
        year: int,
        month: int,
        config: Optional[JobConfig] = None
    ):
        """
        Initialize the taxi data ingestion job.

        Args:
            taxi_type: Type of taxi data to ingest (yellow or green)
            year: Year of the data (2009 or later)
            month: Month of the data (1-12)
            config: Optional job configuration (uses default singleton if not provided)

        Raises:
            ValueError: If taxi_type, year, or month are invalid
        """
        # Validate parameters before calling super().__init__
        self._validate_parameters(taxi_type, year, month)

        super().__init__(
            job_name=f"TaxiIngestion_{taxi_type}_{year}_{month:02d}",
            config=config
        )
        self.taxi_type = taxi_type
        self.year = year
        self.month = month
        self.file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"

    @staticmethod
    def _validate_parameters(
        taxi_type: str,
        year: int,
        month: int
    ) -> None:
        """
        Validate job parameters before initialization.

        Args:
            taxi_type: Type of taxi (yellow or green)
            year: Year of data
            month: Month of data

        Raises:
            ValueError: If any parameter is invalid
        """
        if taxi_type not in TaxiIngestionJob.VALID_TAXI_TYPES:
            raise ValueError(
                f"Invalid taxi_type: {taxi_type}. "
                f"Must be one of {TaxiIngestionJob.VALID_TAXI_TYPES}"
            )

        if not isinstance(year, int) or year < TaxiIngestionJob.MIN_YEAR:
            raise ValueError(
                f"Invalid year: {year}. "
                f"Must be integer >= {TaxiIngestionJob.MIN_YEAR}"
            )

        if not isinstance(month, int) or not 1 <= month <= 12:
            raise ValueError(f"Invalid month: {month}. Must be integer between 1 and 12")

    def validate_inputs(self):
        """
        Validate job inputs
        """
        if self.taxi_type not in ["yellow", "green"]:
            raise ValueError(f"Invalid taxi type: {self.taxi_type}")

        if not 1 <= self.month <= 12:
            raise ValueError(f"Invalid month: {self.month}")

        if self.year < 2009:
            raise ValueError(f"Invalid year: {self.year}")

        self.logger.info(f"Validated inputs: {self.taxi_type}, {self.year}-{self.month:02d}")

    def extract(self) -> DataFrame:
        """
        Extract data from source.
        For ingestion job, always download from NYC TLC to get fresh data.
        """
        return self._extract_from_source()

    def _extract_from_minio(self) -> DataFrame:
        """
        Load data from MinIO bronze layer (used by downstream jobs)
        """
        s3_path = self.config.get_s3_path("bronze", taxi_type=self.taxi_type)
        self.logger.info(f"Loading from MinIO: {s3_path}")
        # Read with partition filters for performance
        partition_path = f"{s3_path}/year={self.year}/month={self.month}"
        return self.spark.read.parquet(partition_path)

    def _extract_from_source(self) -> DataFrame:
        """
        Download and load data from NYC TLC with intelligent caching.

        Caching Strategy:
            1. Check MinIO cache (if enabled)
            2. If cache miss, download from NYC TLC
            3. Upload to MinIO cache for future use
            4. Fallback to local cache if MinIO fails

        Returns:
            DataFrame containing raw trip data from source

        Raises:
            DownloadError: If download from NYC TLC fails
            JobExecutionError: If data cannot be loaded
        """
        # NYC TLC provides monthly parquet files via their CDN
        url = f"{self.NYC_TLC_BASE_URL}/{self.file_name}"

        if self.config.minio.use_minio:
            try:
                return self._extract_with_minio_cache(url)
            except Exception as e:
                self.logger.error(f"MinIO cache operation failed: {e}")
                self.logger.warning("Falling back to local cache")
                # Fall through to local cache

        # Local cache fallback (if MinIO disabled or error)
        return self._extract_with_local_cache(url)

    def _get_minio_client(self) -> Minio:
        """
        Create and return configured MinIO client.

        Returns:
            Initialized Minio client

        Raises:
            JobExecutionError: If MinIO client creation fails
        """
        try:
            endpoint = self.config.minio.endpoint.replace("http://", "").replace("https://", "")
            minio_client = Minio(
                endpoint,
                access_key=self.config.minio.access_key,
                secret_key=self.config.minio.secret_key,
                secure=False
            )
            return minio_client
        except Exception as e:
            raise JobExecutionError(f"Failed to create MinIO client: {e}") from e

    def _extract_with_minio_cache(self, url: str) -> DataFrame:
        """
        Extract data using MinIO cache-first strategy.

        Args:
            url: Source URL for downloading data

        Returns:
            DataFrame loaded from MinIO cache or freshly downloaded

        Raises:
            DownloadError: If download fails
            JobExecutionError: If MinIO operations fail
        """
        cache_object = f"bronze/nyc_taxi/{self.taxi_type}/cache/{self.file_name}"
        s3_cache_path = f"s3a://{self.config.minio.bucket}/{cache_object}"

        minio_client = self._get_minio_client()

        # Check if file exists in MinIO cache
        try:
            minio_client.stat_object(self.config.minio.bucket, cache_object)
            self.logger.info(f"Cache hit - loading from MinIO: {s3_cache_path}")
            return self.spark.read.parquet(s3_cache_path)
        except S3Error as e:
            # File doesn't exist in cache - proceed with download
            self.logger.info(f"Cache miss - will download from NYC TLC")

        # Download to temporary local file
        self.logger.info(f"Downloading from: {url}")
        local_temp_path = Path(self.config.cache_dir)
        local_temp_path.mkdir(parents=True, exist_ok=True)
        local_file = local_temp_path / self.file_name

        try:
            self._download_file(url, local_file)
        except Exception as e:
            raise DownloadError(f"Failed to download {url}: {e}") from e

        # Upload to MinIO cache
        try:
            self.logger.info(f"Uploading to MinIO cache: {s3_cache_path}")
            minio_client.fput_object(
                self.config.minio.bucket,
                cache_object,
                str(local_file),
                content_type="application/octet-stream"
            )
            self.logger.info(f"Successfully cached in MinIO: {cache_object}")
        except S3Error as e:
            self.logger.warning(f"Failed to upload to MinIO cache: {e}")
            # Non-critical - continue with local file

        # Read from MinIO cache (if upload succeeded) or local file
        try:
            # Try MinIO first
            minio_client.stat_object(self.config.minio.bucket, cache_object)
            self.logger.info(f"Reading from MinIO: {s3_cache_path}")
            df = self.spark.read.parquet(s3_cache_path)
        except S3Error:
            # Fall back to local file
            self.logger.info(f"Reading from local file: {local_file}")
            df = self.spark.read.parquet(str(local_file))

        # Clean up local temp file
        try:
            local_file.unlink()
            self.logger.info(f"Cleaned up temporary file: {local_file}")
        except Exception as e:
            self.logger.warning(f"Failed to delete temporary file {local_file}: {e}")

        return df

    def _extract_with_local_cache(self, url: str) -> DataFrame:
        """
        Extract data using local file cache.

        Args:
            url: Source URL for downloading data

        Returns:
            DataFrame loaded from local cache or freshly downloaded

        Raises:
            DownloadError: If download fails
            JobExecutionError: If file cannot be read
        """
        cache_path = Path(self.config.cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        local_file = cache_path / self.file_name

        if not local_file.exists():
            self.logger.info(f"Local cache miss - downloading from: {url}")
            try:
                self._download_file(url, local_file)
            except Exception as e:
                raise DownloadError(f"Failed to download {url}: {e}") from e
        else:
            self.logger.info(f"Cache hit - using local file: {local_file}")

        try:
            return self.spark.read.parquet(str(local_file))
        except Exception as e:
            raise JobExecutionError(f"Failed to read parquet file {local_file}: {e}") from e

    def _download_file(self, url: str, destination: Path) -> None:
        """
        Download file from URL to destination path.

        Args:
            url: Source URL to download from
            destination: Local path to save file

        Raises:
            DownloadError: If download fails or HTTP error occurs
        """
        try:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()

            file_size = int(response.headers.get('content-length', 0))
            self.logger.info(f"Downloading {file_size:,} bytes from {url}")

            downloaded = 0
            with open(destination, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

            self.logger.info(f"Downloaded {downloaded:,} bytes to {destination}")

        except requests.exceptions.Timeout as e:
            raise DownloadError(f"Download timeout after 300s: {url}") from e
        except requests.exceptions.HTTPError as e:
            raise DownloadError(f"HTTP error {e.response.status_code}: {url}") from e
        except requests.exceptions.RequestException as e:
            raise DownloadError(f"Download failed: {e}") from e
        except IOError as e:
            raise DownloadError(f"Failed to write file {destination}: {e}") from e

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform the data with minimal changes (Bronze layer principle).

        Bronze Layer Transformations:
        1. Preserve raw data - no business logic transformations
        2. Schema enforcement - validate and enforce data types
        3. Add metadata columns for lineage and CDC tracking
        4. Data quality validation - filter to requested year/month

        This ensures we maintain the raw, unaltered source data while adding
        necessary metadata for downstream processing and auditing.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import TimestampType, DoubleType, IntegerType

        record_count = df.count()
        self.logger.info(f"Processing {record_count:,} records before validation")
        self.logger.info(f"Schema: {df.schema}")

        # Schema enforcement - ensure critical columns have correct types
        # Identify the pickup datetime column (varies by taxi type)
        pickup_col = None
        if "tpep_pickup_datetime" in df.columns:
            pickup_col = "tpep_pickup_datetime"
        elif "lpep_pickup_datetime" in df.columns:
            pickup_col = "lpep_pickup_datetime"
        else:
            raise ValueError("No pickup datetime column found in data")

        # Enforce datetime type on pickup column if not already
        if df.schema[pickup_col].dataType != TimestampType():
            self.logger.warning(f"Converting {pickup_col} to timestamp type")
            df = df.withColumn(pickup_col, F.col(pickup_col).cast(TimestampType()))

        # Data quality validation - extract year/month for filtering
        df = df.withColumn("pickup_year", F.year(F.col(pickup_col))) \
               .withColumn("pickup_month", F.month(F.col(pickup_col)))

        # Log data distribution before filtering
        year_month_dist = df.groupBy("pickup_year", "pickup_month").count().collect()
        self.logger.info(f"Data distribution by year/month: {year_month_dist}")

        # Filter to only include records matching the requested year and month
        df_filtered = df.filter(
            (F.col("pickup_year") == self.year) &
            (F.col("pickup_month") == self.month)
        )

        filtered_count = df_filtered.count()
        self.logger.info(f"Filtered to {filtered_count:,} records for {self.year}-{self.month:02d}")

        if filtered_count == 0:
            raise ValueError(
                f"No records found for {self.year}-{self.month:02d}. "
                f"Source file may contain incorrect data."
            )

        # Validate that we're getting reasonable data
        match_percentage = (filtered_count / record_count) * 100 if record_count > 0 else 0
        self.logger.info(f"Match percentage: {match_percentage:.1f}%")

        if match_percentage < self.MIN_MATCH_PERCENTAGE:
            self.logger.warning(
                f"Low match percentage ({match_percentage:.1f}%). "
                f"Expected >{self.MIN_MATCH_PERCENTAGE}% for {self.year}-{self.month:02d}"
            )

        # Add metadata columns ONLY (no business logic transformations)
        # These metadata columns enable:
        # - Data lineage tracking (source_file, ingestion_timestamp)
        # - CDC operations (record_hash, ingestion_date)
        # - Partitioning (year, month)
        df_final = df_filtered.withColumn("ingestion_timestamp", F.current_timestamp()) \
                              .withColumn("ingestion_date", F.current_date()) \
                              .withColumn("source_file", F.lit(self.file_name)) \
                              .withColumn("year", F.lit(self.year)) \
                              .withColumn("month", F.lit(self.month)) \
                              .withColumn("record_hash", F.hash(*df.columns))  # For CDC change detection

        # Drop temporary columns used for validation
        df_final = df_final.drop("pickup_year", "pickup_month")

        self.logger.info(f"Bronze layer transformation complete: {filtered_count:,} records")
        self.logger.info("Transformations applied: schema enforcement, metadata addition only")

        return df_final

    def load(self, df: DataFrame):
        """
        Load data to MinIO bronze layer with partitioning.
        Partitioned by year and month for:
        - Delta Lake compatibility
        - CDC (Change Data Capture) tracking
        - Query performance optimization
        - Partition pruning
        """
        # Cache the dataframe and count records before writing
        df = df.cache()
        record_count = df.count()

        if self.config.minio.use_minio:
            s3_path = self.config.get_s3_path("bronze", taxi_type=self.taxi_type)
            self.logger.info(f"Writing {record_count:,} records to MinIO bronze layer: {s3_path}")
            self.logger.info(f"Partitioning by: year={self.year}, month={self.month}")

            # Partition by year and month for optimal performance and delta operations
            df.write \
                .mode("append") \
                .partitionBy("year", "month") \
                .option("compression", "snappy") \
                .parquet(s3_path)

            self.logger.info(f"Successfully loaded {record_count:,} records to bronze layer")
            self.logger.info(f"Path: {s3_path}/year={self.year}/month={self.month}/")
        else:
            # If not using MinIO, save locally with partitioning
            output_path = f"{self.config.cache_dir}/output/{self.taxi_type}"
            self.logger.info(f"Writing {record_count:,} records to local path: {output_path}")
            df.write \
                .mode("append") \
                .partitionBy("year", "month") \
                .option("compression", "snappy") \
                .parquet(output_path)

        df.unpersist()


def run_ingestion(
    taxi_type: Literal["yellow", "green"],
    year: int,
    month: int
) -> bool:
    """
    Convenience function to run the ingestion job for a single month.
    :param taxi_type: Type of taxi (yellow or green)
    :param year: Year of the data
    :param month: Month of the data
    :returns bool: True if successful, False otherwise
    """
    job = TaxiIngestionJob(taxi_type, year, month)
    return job.run()


def run_bulk_ingestion(
    taxi_type: Literal["yellow", "green"],
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int
) -> dict:
    """
    Run ingestion for multiple months (historical data ingestion).
    :param taxi_type: Type of taxi (yellow or green)
    :param start_year: Starting year
    :param start_month: Starting month (1-12)
    :param end_year: Ending year
    :param end_month: Ending month (1-12)
    :returns dict: Dictionary with results for each month
    """
    from datetime import date
    from dateutil.relativedelta import relativedelta
    import logging

    logger = logging.getLogger(__name__)

    # Generate list of (year, month) tuples
    current_date = date(start_year, start_month, 1)
    end_date = date(end_year, end_month, 1)

    results = {}
    total_months = 0
    successful = 0
    failed = 0

    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        total_months += 1

        logger.info(f"Processing {taxi_type} taxi data for {year}-{month:02d} ({total_months})")

        try:
            success = run_ingestion(taxi_type, year, month)
            results[f"{year}-{month:02d}"] = "SUCCESS" if success else "FAILED"
            if success:
                successful += 1
            else:
                failed += 1
        except Exception as e:
            logger.error(f"Error processing {year}-{month:02d}: {e}")
            results[f"{year}-{month:02d}"] = f"ERROR: {str(e)}"
            failed += 1

        # Move to next month
        current_date += relativedelta(months=1)

    logger.info(f"Bulk ingestion complete: {successful}/{total_months} successful, {failed} failed")
    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC Taxi Data Ingestion Job")
    parser.add_argument("--taxi-type", type=str, choices=["yellow", "green"], required=True)
    parser.add_argument("--year", type=int, help="Year of the data (for single month ingestion)")
    parser.add_argument("--month", type=int, help="Month of the data (for single month ingestion)")
    parser.add_argument("--start-year", type=int, help="Start year (for bulk ingestion)")
    parser.add_argument("--start-month", type=int, help="Start month (for bulk ingestion)")
    parser.add_argument("--end-year", type=int, help="End year (for bulk ingestion)")
    parser.add_argument("--end-month", type=int, help="End month (for bulk ingestion)")

    args = parser.parse_args()

    # Determine if single or bulk ingestion
    if args.year and args.month:
        # Single month ingestion
        success = run_ingestion(args.taxi_type, args.year, args.month)
        exit(0 if success else 1)
    elif args.start_year and args.start_month and args.end_year and args.end_month:
        # Bulk ingestion
        results = run_bulk_ingestion(
            args.taxi_type,
            args.start_year,
            args.start_month,
            args.end_year,
            args.end_month
        )
        # Print summary
        print("\n=== Bulk Ingestion Results ===")
        for period, result in results.items():
            print(f"{period}: {result}")

        # Exit with success if all succeeded
        failed_count = sum(1 for r in results.values() if r != "SUCCESS")
        exit(0 if failed_count == 0 else 1)
    else:
        parser.error("Either provide --year and --month for single ingestion, or --start-year, --start-month, --end-year, --end-month for bulk ingestion")
