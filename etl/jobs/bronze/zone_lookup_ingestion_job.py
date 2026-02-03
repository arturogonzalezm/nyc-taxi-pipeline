"""
NYC Taxi Zone Lookup Ingestion Job - Reference Data ETL.

This module implements the bronze pipeline for NYC Taxi zone lookup reference data.
This is a static reference table that maps LocationID to Borough, Zone, and service_zone.

Purpose:
    - Ingest reference data for dimensional modeling
    - Enable zone-based analytics and aggregations
    - Support downstream join operations in Silver/Gold layers
    - Maintain raw CSV format for auditability

Architecture:
    - Downloads zone lookup CSV from NYC TLC CDN
    - Validates required columns and data quality
    - Uploads raw CSV to MinIO misc directory (not bronze layer)
    - Preserves original format for maximum compatibility

Data Quality:
    - Validates required columns exist
    - Checks for null LocationIDs (primary key)
    - Logs data sample and schema information
    - No transformations - raw data only

Design Pattern:
    - Template Method: Inherits from BaseSparkJob
    - Uses MinIO Python client for direct CSV upload
"""

import requests
import logging
from pathlib import Path
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from minio import Minio
from minio.error import S3Error
from google.cloud import storage as gcs_storage

from ..base_job import BaseSparkJob, JobExecutionError
from ..utils.config import JobConfig

logger = logging.getLogger(__name__)


class ReferenceDataError(JobExecutionError):
    """
    Raised when reference data validation fails
    """

    pass


class ZoneLookupIngestionJob(BaseSparkJob):
    """
    Production-ready bronze job for NYC Taxi zone lookup reference data.

    This job downloads the taxi zone lookup CSV, validates it, and uploads the
    raw CSV file to MinIO misc directory for use as reference/dimension data.

    Features:
        - Downloads from NYC TLC CDN
        - Validates required columns and primary key
        - Preserves raw CSV format (no transformations)
        - Direct upload to MinIO using Python client
        - Local cache for offline development

    Reference Data Schema:
        - LocationID: Primary key, integer, unique zone identifier
        - Borough: Borough name (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR)
        - Zone: Specific zone name within borough
        - service_zone: Service zone classification (Yellow, Green, Boro)

    Example:
        >>> job = ZoneLookupIngestionJob()
        >>> success = job.run()
        >>> # File uploaded to s3a://nyc-taxi-pipeline/misc/taxi_zone_lookup.csv

    Attributes:
        file_name: Name of the zone lookup CSV file
        source_url: NYC TLC CDN URL for zone lookup data
    """

    # Class constants
    FILE_NAME = "taxi_zone_lookup.csv"
    SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    REQUIRED_COLUMNS = ["LocationID", "Borough", "Zone", "service_zone"]
    MINIO_OBJECT_PATH = f"misc/{FILE_NAME}"

    def __init__(self, config: Optional[JobConfig] = None):
        """
        Initialise the zone lookup bronze job.
        :params config: Optional job configuration (uses default singleton if not provided)
        """
        super().__init__(job_name="ZoneLookupIngestion", config=config)
        self.file_name = self.FILE_NAME
        self.source_url = self.SOURCE_URL

    def validate_inputs(self):
        """
        Validate job inputs
        """
        self.logger.info(f"Validating zone lookup bronze from {self.source_url}")

    def extract(self) -> DataFrame:
        """
        Extract zone lookup data from NYC TLC.
        Downloads CSV file and loads into Spark DataFrame.
        """
        return self._extract_from_source()

    def _extract_from_source(self) -> DataFrame:
        """
        Download and load zone lookup CSV from NYC TLC.
        :returns: DataFrame containing zone lookup reference data
        :raises JobExecutionError: If download or file read fails
        """
        cache_path = Path(self.config.cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        local_file = cache_path / self.file_name

        if not local_file.exists():
            self.logger.info(f"Downloading zone lookup from: {self.source_url}")

            try:
                response = requests.get(self.source_url, stream=True, timeout=60)
                response.raise_for_status()

                file_size = int(response.headers.get("content-length", 0))
                self.logger.info(f"Downloading {file_size:,} bytes")

                # Download CSV file
                with open(local_file, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                self.logger.info(f"Downloaded to {local_file}")

            except requests.exceptions.RequestException as e:
                raise JobExecutionError(f"Failed to download zone lookup: {e}") from e
            except IOError as e:
                raise JobExecutionError(
                    f"Failed to write file {local_file}: {e}"
                ) from e
        else:
            self.logger.info(f"Using cached file: {local_file}")

        # Read CSV with header and schema inference
        try:
            df = self.spark.read.csv(str(local_file), header=True, inferSchema=True)
        except Exception as e:
            raise JobExecutionError(f"Failed to read CSV {local_file}: {e}") from e

        record_count = df.count()
        self.logger.info(f"Loaded {record_count:,} zone records")

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Validate the zone lookup reference data.

        Reference data validation only - no transformations applied.
        This ensures data quality while preserving the raw reference data.

        Validations:
            - Required columns exist
            - Primary key (LocationID) has no nulls
            - Record count is reasonable (>0)

        :returns: Original DataFrame unmodified (validation only)
        :raises ReferenceDataError: If validation fails
        """
        self.logger.info("Validating zone lookup reference data")
        self.logger.info(f"Schema: {df.schema}")

        # Display sample of data for verification
        self.logger.info("Sample data:")
        df.show(5, truncate=False)

        # Validate required columns exist
        missing_columns = [
            col for col in self.REQUIRED_COLUMNS if col not in df.columns
        ]

        if missing_columns:
            raise ReferenceDataError(
                f"Missing required columns: {missing_columns}. "
                f"Expected columns: {self.REQUIRED_COLUMNS}"
            )

        self.logger.info(f"All required columns present: {self.REQUIRED_COLUMNS}")

        # Check for null LocationIDs (primary key constraint)
        null_location_count = df.filter(F.col("LocationID").isNull()).count()
        if null_location_count > 0:
            raise ReferenceDataError(
                f"Found {null_location_count} records with null LocationID. "
                "LocationID is primary key and cannot be null."
            )

        # Validate record count
        record_count = df.count()
        if record_count == 0:
            raise ReferenceDataError("Zone lookup data is empty (0 records)")

        self.logger.info(f"Zone lookup validation passed: {record_count:,} records")

        # Check for duplicates in LocationID
        duplicate_count = (
            df.groupBy("LocationID").count().filter(F.col("count") > 1).count()
        )
        if duplicate_count > 0:
            self.logger.warning(
                f"Found {duplicate_count} duplicate LocationIDs. "
                "Primary key should be unique."
            )

        # Return original dataframe without any modifications
        return df

    def load(self, df: DataFrame):
        """
        Load zone lookup data to cloud storage (GCS or MinIO) misc layer as raw CSV file.

        This method uploads the raw CSV file (not the DataFrame) to cloud storage
        to preserve the original format and ensure maximum compatibility.
        :params df: Validated DataFrame (used for record count only)
        :raises FileNotFoundError: If local cache file doesn't exist
        :raises JobExecutionError: If cloud storage upload fails
        """
        record_count = df.count()
        self.logger.info(f"Validated {record_count:,} zone records, preparing upload")

        # Get the local cached file
        cache_path = Path(self.config.cache_dir)
        local_file = cache_path / self.file_name

        if not local_file.exists():
            raise FileNotFoundError(
                f"Local file not found: {local_file}. "
                "Cannot upload to cloud storage without cached file."
            )

        if self.config.use_gcs:
            # Upload to Google Cloud Storage
            try:
                gcs_client = gcs_storage.Client(project=self.config.gcs.project_id)
                bucket = gcs_client.bucket(self.config.gcs.bucket)
                blob = bucket.blob(self.MINIO_OBJECT_PATH)

                self.logger.info(
                    f"Uploading {local_file} to gs://{self.config.gcs.bucket}/{self.MINIO_OBJECT_PATH}"
                )

                blob.upload_from_filename(str(local_file), content_type="text/csv")

                self.logger.info(f"Successfully uploaded {self.file_name} to GCS")
                self.logger.info(
                    f"GCS path: gs://{self.config.gcs.bucket}/{self.MINIO_OBJECT_PATH}"
                )

            except Exception as e:
                self.logger.error(f"GCS upload error: {e}")
                raise JobExecutionError(f"Failed to upload to GCS: {e}") from e

        elif self.config.minio.use_minio:
            # Upload raw CSV file directly to MinIO
            try:
                minio_client = self._get_minio_client()
                bucket_name = self.config.minio.bucket

                # Ensure bucket exists (create if needed)
                self._ensure_bucket_exists(minio_client, bucket_name)

                # Upload file to misc directory
                self.logger.info(
                    f"Uploading {local_file} to {bucket_name}/{self.MINIO_OBJECT_PATH}"
                )

                minio_client.fput_object(
                    bucket_name,
                    self.MINIO_OBJECT_PATH,
                    str(local_file),
                    content_type="text/csv",
                )

                self.logger.info(f"Successfully uploaded {self.file_name} to MinIO")
                self.logger.info(
                    f"MinIO path: s3a://{bucket_name}/{self.MINIO_OBJECT_PATH}"
                )

            except S3Error as e:
                self.logger.error(f"MinIO S3 error: {e}")
                raise JobExecutionError(f"Failed to upload to MinIO: {e}") from e
            except Exception as e:
                self.logger.error(f"Error uploading to MinIO: {e}")
                raise JobExecutionError(f"MinIO upload failed: {e}") from e
        else:
            # If not using cloud storage, just keep it in cache
            cache_file_path = Path(self.config.cache_dir) / self.file_name
            self.logger.info(f"File available in local cache: {cache_file_path}")
            self.logger.info(
                "Cloud storage upload skipped (STORAGE_BACKEND not configured)"
            )

    def _get_minio_client(self) -> Minio:
        """
        Create and return configured MinIO client.
        :returns: Initialised Minio client
        :raises JobExecutionError: If MinIO client creation fails
        """
        try:
            endpoint = self.config.minio.endpoint
            # Remove http:// or https:// prefix if present
            endpoint = endpoint.replace("http://", "").replace("https://", "")

            minio_client = Minio(
                endpoint,
                access_key=self.config.minio.access_key,
                secret_key=self.config.minio.secret_key,
                secure=False,  # Set to True if using HTTPS
            )

            self.logger.info(f"MinIO client initialized: {endpoint}")
            return minio_client

        except Exception as e:
            raise JobExecutionError(f"Failed to create MinIO client: {e}") from e

    def _ensure_bucket_exists(self, minio_client: Minio, bucket_name: str) -> None:
        """
        Ensure MinIO bucket exists, create if it doesn't.
        :params minio_client: Initialized MinIO client
        :params bucket_name: Name of bucket to check/create
        :raises JobExecutionError: If bucket check or creation fails
        """
        try:
            if not minio_client.bucket_exists(bucket_name):
                self.logger.info(f"Bucket does not exist, creating: {bucket_name}")
                minio_client.make_bucket(bucket_name)
                self.logger.info(f"Created bucket: {bucket_name}")
            else:
                self.logger.info(f"Bucket exists: {bucket_name}")

        except S3Error as e:
            raise JobExecutionError(
                f"Failed to check/create bucket {bucket_name}: {e}"
            ) from e


def run_zone_lookup_ingestion() -> bool:
    """
    Convenience function to run the zone lookup bronze job.
    :returns: True if job completed successfully, False otherwise
    """
    job = ZoneLookupIngestionJob()
    return job.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC Taxi Zone Lookup Ingestion Job")
    args = parser.parse_args()

    success = run_zone_lookup_ingestion()
    exit(0 if success else 1)
