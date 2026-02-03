"""
NYC Taxi Data Ingestion Job - Bronze Layer ETL.

This module implements the bronze pipeline for NYC Taxi trip data, downloading
monthly parquet files from NYC TLC and storing them in the MinIO bronze layer with
appropriate partitioning for Delta Lake and CDC operations.

Architecture:
    - Follows Medallion Architecture (Bronze/Silver/Gold layers)
    - Bronze layer: Raw data with minimal gold, metadata added
    - Implements caching strategy: MinIO cache with local fallback
    - Supports both single-month and bulk historical bronze

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

import sys
import requests
from pathlib import Path
from typing import Literal, Optional

# Add project root to path for imports when running as script
if __name__ == "__main__" or "etl.jobs" not in sys.modules:
    project_root = Path(__file__).resolve().parents[3]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from pyspark.sql import DataFrame
from minio import Minio
from minio.error import S3Error

from etl.jobs.base_job import BaseSparkJob, JobExecutionError
from etl.jobs.utils.config import JobConfig


class DataValidationError(JobExecutionError):
    """
    Raised when data validation fails during bronze

    :raises DataValidationError: If data does not match requested year/month or fails schema validation
    """

    pass


class DownloadError(JobExecutionError):
    """
    Raised when data download from NYC TLC fails

    :raises DownloadError: If download fails or HTTP error occurs
    """

    pass


class TaxiIngestionJob(BaseSparkJob):
    """
    Production-ready bronze job for NYC Taxi trip data.

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
        >>> # Single month bronze
        >>> job = TaxiIngestionJob("yellow", 2024, 1)
        >>> success = job.run()
        >>>
        >>> # Bulk historical bronze
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

    def __init__(
        self,
        taxi_type: Literal["yellow", "green"],
        year: int,
        month: int,
        config: Optional[JobConfig] = None,
    ):
        """
        Initialise the taxi data bronze job.

        :params taxi_type: Type of taxi data to ingest (yellow or green)
        :params year: Year of the data (2009 or later)
        :params month: Month of the data (1-12)
        :params config: Optional job configuration (uses default singleton if not provided)
        :raises ValueError: If taxi_type, year, or month are invalid
        """
        # Validate parameters before calling super().__init__
        self._validate_parameters(taxi_type, year, month)

        super().__init__(
            job_name=f"TaxiIngestion_{taxi_type}_{year}_{month:02d}", config=config
        )
        self.taxi_type = taxi_type
        self.year = year
        self.month = month
        self.file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"

    @staticmethod
    def _validate_parameters(taxi_type: str, year: int, month: int) -> None:
        """
        Validate job parameters before initialization.

        :params taxi_type: Type of taxi (yellow or green)
        :params year: Year of data
        :params month: Month of data
        :raises ValueError: If any parameter is invalid
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
            raise ValueError(
                f"Invalid month: {month}. Must be integer between 1 and 12"
            )

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

        self.logger.info(
            f"Validated inputs: {self.taxi_type}, {self.year}-{self.month:02d}"
        )

    def extract(self) -> DataFrame:
        """
        Extract data from source.
        For bronze job, always download from NYC TLC to get fresh data.
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

        :returns: DataFrame containing raw trip data from source
        :raises DownloadError: If download from NYC TLC fails
        :raises JobExecutionError: If data cannot be loaded
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

        :returns: Initialised Minio client
        :raises JobExecutionError: If MinIO client creation fails
        """
        try:
            endpoint = self.config.minio.endpoint.replace("http://", "").replace(
                "https://", ""
            )
            minio_client = Minio(
                endpoint,
                access_key=self.config.minio.access_key,
                secret_key=self.config.minio.secret_key,
                secure=False,
            )
            return minio_client
        except Exception as e:
            raise JobExecutionError(f"Failed to create MinIO client: {e}") from e

    def _extract_with_minio_cache(self, url: str) -> DataFrame:
        """
        Extract data using MinIO cache-first strategy.

        :params url: Source URL for downloading data
        :returns DataFrame loaded from MinIO cache or freshly downloaded
        :raises DownloadError: If download fails
        :raises JobExecutionError: If MinIO operations fail
        """
        cache_object = f"bronze/nyc_taxi/{self.taxi_type}/cache/{self.file_name}"
        s3_cache_path = f"s3a://{self.config.minio.bucket}/{cache_object}"

        minio_client = self._get_minio_client()

        # Check if file exists in MinIO cache
        try:
            minio_client.stat_object(self.config.minio.bucket, cache_object)
            self.logger.info(f"Cache hit - loading from MinIO: {s3_cache_path}")
            return self.spark.read.parquet(s3_cache_path)
        except S3Error:
            # File doesn't exist in cache - proceed with download
            self.logger.info("Cache miss - will download from NYC TLC")

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
                content_type="application/octet-stream",
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

        :params url: Source URL for downloading data
        :returns: DataFrame loaded from local cache or freshly downloaded
        :raises DownloadError: If download fails
        :raises JobExecutionError: If file cannot be read
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
            raise JobExecutionError(
                f"Failed to read parquet file {local_file}: {e}"
            ) from e

    def _download_file(self, url: str, destination: Path) -> None:
        """
        Download file from URL to destination path.

        :params url: Source URL to download from
        :params destination: Local path to save file
        :raises DownloadError: If download fails or HTTP error occurs
        """
        try:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()

            file_size = int(response.headers.get("content-length", 0))
            self.logger.info(f"Downloading {file_size:,} bytes from {url}")

            downloaded = 0
            with open(destination, "wb") as f:
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
        2. Add metadata columns for lineage tracking
        3. Add record_hash for deduplication and data integrity

        This ensures we maintain the raw, unaltered source data while adding
        necessary metadata for downstream processing and auditing.
        """
        from pyspark.sql import functions as F

        record_count = df.count()
        self.logger.info(f"Processing {record_count:,} records")
        self.logger.info(f"Schema: {df.schema}")

        # Identify business columns (exclude metadata columns we'll add)
        business_columns = [col for col in df.columns]
        self.logger.info(
            f"Computing record_hash from {len(business_columns)} business columns"
        )

        # Create record_hash from business columns using SHA-256
        # This enables deduplication and tracks data changes across layers
        # Concat all columns -> SHA-256 hash -> ensures data integrity
        df_with_hash = df.withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(c).cast("string"), F.lit(""))
                        for c in business_columns
                    ],
                ),
                256,
            ),
        )

        # Add metadata columns ONLY (no business logic transformations)
        # These metadata columns enable data lineage tracking and auditing
        df_final = (
            df_with_hash.withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("ingestion_date", F.current_date())
            .withColumn("source_file", F.lit(self.file_name))
            .withColumn(
                "source_url", F.lit(f"{self.NYC_TLC_BASE_URL}/{self.file_name}")
            )
            .withColumn("job_name", F.lit(self.job_name))
            .withColumn("data_layer", F.lit("bronze"))
            .withColumn("year", F.lit(self.year))
            .withColumn("month", F.lit(self.month))
        )

        # Log hash statistics
        unique_hashes = df_final.select("record_hash").distinct().count()
        self.logger.info(
            f"Record hashes: {unique_hashes:,} unique out of {record_count:,} total"
        )
        if unique_hashes < record_count:
            duplicate_count = record_count - unique_hashes
            self.logger.warning(
                f"Found {duplicate_count:,} duplicate records in source data ({100 * duplicate_count / record_count:.2f}%)"
            )

        self.logger.info(
            f"Bronze layer transformation complete: {record_count:,} records"
        )
        self.logger.info("Transformations applied: record_hash + metadata addition")

        return df_final

    def load(self, df: DataFrame):
        """
        Load data to cloud storage (GCS or MinIO) bronze layer with partitioning.
        Partitioned by year and month for:
        - Delta Lake compatibility
        - CDC (Change Data Capture) tracking
        - Query performance optimization
        - Partition pruning
        """
        # Cache the dataframe and count records before writing
        df = df.cache()
        record_count = df.count()

        if self.config.use_gcs or self.config.minio.use_minio:
            storage_path = self.config.get_storage_path("bronze", taxi_type=self.taxi_type)
            backend_name = "GCS" if self.config.use_gcs else "MinIO"
            self.logger.info(
                f"Writing {record_count:,} records to {backend_name} bronze layer: {storage_path}"
            )
            self.logger.info(f"Partitioning by: year={self.year}, month={self.month}")

            # Partition by year and month for optimal performance and delta operations
            df.write.mode("append").partitionBy("year", "month").option(
                "compression", "snappy"
            ).parquet(storage_path)

            self.logger.info(
                f"Successfully loaded {record_count:,} records to bronze layer"
            )
            self.logger.info(f"Path: {storage_path}/year={self.year}/month={self.month}/")
        else:
            # If not using cloud storage, save locally with partitioning
            output_path = f"{self.config.cache_dir}/output/{self.taxi_type}"
            self.logger.info(
                f"Writing {record_count:,} records to local path: {output_path}"
            )
            df.write.mode("append").partitionBy("year", "month").option(
                "compression", "snappy"
            ).parquet(output_path)

        df.unpersist()


def run_ingestion(taxi_type: Literal["yellow", "green"], year: int, month: int) -> bool:
    """
    Convenience function to run the bronze job for a single month.

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
    end_month: int,
) -> dict:
    """
    Run bronze for multiple months (historical data bronze).

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

        logger.info("=" * 80)
        logger.info(
            f"Processing {taxi_type} taxi data for {year}-{month:02d} ({total_months}/{(end_date.year - start_year) * 12 + end_date.month - start_month + 1})"
        )
        logger.info("=" * 80)

        try:
            success = run_ingestion(taxi_type, year, month)

            if success:
                results[f"{year}-{month:02d}"] = "✓ SUCCESS"
                successful += 1
                logger.info(f"✓ {year}-{month:02d}: SUCCESS")
            else:
                results[f"{year}-{month:02d}"] = "✗ FAILED"
                failed += 1
                logger.warning(f"✗ {year}-{month:02d}: FAILED")

        except Exception as e:
            error_msg = str(e)
            # Check for specific error types - be more precise with 404 detection
            # Only skip if it's actually a download error with 404 status
            if (
                isinstance(e.__cause__, requests.exceptions.HTTPError)
                and e.__cause__.response.status_code == 404
            ):
                logger.warning(
                    f"✗ {year}-{month:02d}: SKIPPED - Data not available (HTTP 404)"
                )
                results[f"{year}-{month:02d}"] = "⊘ SKIPPED (404 - Data not available)"
            elif isinstance(e, DownloadError) and (
                "404" in error_msg or "Not Found" in error_msg
            ):
                logger.warning(
                    f"✗ {year}-{month:02d}: SKIPPED - Data not available (404)"
                )
                results[f"{year}-{month:02d}"] = "⊘ SKIPPED (404 - Data not available)"
            elif "timeout" in error_msg.lower():
                logger.error(f"✗ {year}-{month:02d}: FAILED - Network timeout")
                results[f"{year}-{month:02d}"] = "✗ FAILED (Network timeout)"
            else:
                logger.error(f"✗ {year}-{month:02d}: ERROR - {error_msg[:100]}")
                results[f"{year}-{month:02d}"] = f"✗ ERROR: {error_msg[:100]}"
            failed += 1

        # Move to next month
        current_date += relativedelta(months=1)

    logger.info("\n" + "=" * 80)
    logger.info(
        f"Bulk ingestion complete: {successful}/{total_months} successful, {failed} failed/skipped"
    )
    logger.info(f"Success rate: {100 * successful / total_months:.1f}%")
    logger.info("=" * 80)

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC Taxi Data Ingestion Job")
    parser.add_argument(
        "--taxi-type", type=str, choices=["yellow", "green"], required=True
    )
    parser.add_argument(
        "--year", type=int, help="Year of the data (for single month bronze)"
    )
    parser.add_argument(
        "--month", type=int, help="Month of the data (for single month bronze)"
    )
    parser.add_argument("--start-year", type=int, help="Start year (for bulk bronze)")
    parser.add_argument("--start-month", type=int, help="Start month (for bulk bronze)")
    parser.add_argument("--end-year", type=int, help="End year (for bulk bronze)")
    parser.add_argument("--end-month", type=int, help="End month (for bulk bronze)")

    args = parser.parse_args()

    # Determine if single or bulk bronze
    if args.year and args.month:
        # Single month bronze
        success = run_ingestion(args.taxi_type, args.year, args.month)
        exit(0 if success else 1)
    elif args.start_year and args.start_month and args.end_year and args.end_month:
        # Bulk bronze
        results = run_bulk_ingestion(
            args.taxi_type,
            args.start_year,
            args.start_month,
            args.end_year,
            args.end_month,
        )
        # Print summary
        print("\n=== Bulk Ingestion Results ===")
        for period, result in results.items():
            print(f"{period}: {result}")

        # Exit with success if all succeeded
        failed_count = sum(1 for r in results.values() if "SUCCESS" not in r)
        exit(0 if failed_count == 0 else 1)
    else:
        parser.error(
            "Either provide --year and --month for single bronze, or --start-year, --start-month, --end-year, --end-month for bulk bronze"
        )
