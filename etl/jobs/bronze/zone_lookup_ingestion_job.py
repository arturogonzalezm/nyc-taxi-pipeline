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
    - Uploads raw CSV to GCS misc directory
    - Preserves original format for maximum compatibility

Data Quality:
    - Validates required columns exist
    - Checks for null LocationIDs (primary key)
    - Logs data sample and schema information
    - No transformations - raw data only

Design Pattern:
    - Template Method: Inherits from BaseSparkJob
    - Uses GCS Python client for direct CSV upload
"""

import requests
import logging
from pathlib import Path
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
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
    raw CSV file to GCS misc directory for use as reference/dimension data.

    Features:
        - Downloads from NYC TLC CDN
        - Validates required columns and primary key
        - Preserves raw CSV format (no transformations)
        - Direct upload to GCS using Python client
        - Local cache for offline development

    Reference Data Schema:
        - LocationID: Primary key, integer, unique zone identifier
        - Borough: Borough name (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR)
        - Zone: Specific zone name within borough
        - service_zone: Service zone classification (Yellow, Green, Boro)

    Example:
        >>> job = ZoneLookupIngestionJob()
        >>> success = job.run()
        >>> # File uploaded to gs://bucket/misc/taxi_zone_lookup.csv

    Attributes:
        file_name: Name of the zone lookup CSV file
        source_url: NYC TLC CDN URL for zone lookup data
    """

    # Class constants
    FILE_NAME = "taxi_zone_lookup.csv"
    SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    REQUIRED_COLUMNS = ["LocationID", "Borough", "Zone", "service_zone"]
    GCS_OBJECT_PATH = f"misc/{FILE_NAME}"

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
        Download zone lookup CSV from NYC TLC and load into DataFrame.

        Uses local caching to avoid repeated downloads during development.

        :returns: DataFrame with zone lookup data
        :raises ReferenceDataError: If download or parsing fails
        """
        cache_path = Path(self.config.cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        local_file = cache_path / self.file_name

        # Download if not cached
        if not local_file.exists():
            self.logger.info(f"Downloading zone lookup from {self.source_url}")
            try:
                response = requests.get(self.source_url, timeout=60)
                response.raise_for_status()

                with open(local_file, "wb") as f:
                    f.write(response.content)

                self.logger.info(f"Downloaded {len(response.content):,} bytes")

            except requests.exceptions.RequestException as e:
                raise ReferenceDataError(
                    f"Failed to download zone lookup: {e}"
                ) from e
        else:
            self.logger.info(f"Using cached file: {local_file}")

        # Load CSV into DataFrame
        try:
            df = self.spark.read.option("header", "true").csv(str(local_file))

            record_count = df.count()
            self.logger.info(f"Loaded {record_count:,} zone records")
            self.logger.info(f"Schema: {df.schema}")

            # Log sample data
            self.logger.info("Sample data:")
            df.show(5, truncate=False)

            return df

        except Exception as e:
            raise ReferenceDataError(f"Failed to parse zone lookup CSV: {e}") from e

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Validate zone lookup data (no transformations for reference data).

        Bronze layer principle: preserve raw data, only validate.

        Validations:
            1. Required columns exist
            2. No null LocationIDs (primary key)
            3. Log duplicate LocationIDs (warning only)

        :params df: Raw zone lookup DataFrame
        :returns: Validated DataFrame (unchanged)
        :raises ReferenceDataError: If validation fails
        """
        self.logger.info("Validating zone lookup reference data")

        # Check required columns
        missing_columns = set(self.REQUIRED_COLUMNS) - set(df.columns)
        if missing_columns:
            raise ReferenceDataError(
                f"Missing required columns: {missing_columns}. "
                f"Found columns: {df.columns}"
            )

        self.logger.info(f"All required columns present: {self.REQUIRED_COLUMNS}")

        # Check for null LocationIDs (primary key validation)
        null_location_count = df.filter(F.col("LocationID").isNull()).count()
        if null_location_count > 0:
            raise ReferenceDataError(
                f"Found {null_location_count} records with null LocationID. "
                "LocationID is the primary key and cannot be null."
            )

        self.logger.info("Primary key validation passed: no null LocationIDs")

        # Check for duplicate LocationIDs (warning only - log but don't fail)
        total_count = df.count()
        unique_count = df.select("LocationID").distinct().count()
        duplicate_count = total_count - unique_count

        self.logger.info(
            f"LocationID uniqueness: {unique_count:,} unique out of {total_count:,} total"
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
        Load zone lookup data to GCS misc layer as raw CSV file.

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

        # Upload to Google Cloud Storage
        try:
            gcs_client = gcs_storage.Client(project=self.config.gcs.project_id)
            bucket = gcs_client.bucket(self.config.gcs.bucket)
            blob = bucket.blob(self.GCS_OBJECT_PATH)

            self.logger.info(
                f"Uploading {local_file} to gs://{self.config.gcs.bucket}/{self.GCS_OBJECT_PATH}"
            )

            blob.upload_from_filename(str(local_file), content_type="text/csv")

            self.logger.info(f"Successfully uploaded {self.file_name} to GCS")
            self.logger.info(
                f"GCS path: gs://{self.config.gcs.bucket}/{self.GCS_OBJECT_PATH}"
            )

        except Exception as e:
            self.logger.error(f"GCS upload error: {e}")
            raise JobExecutionError(f"Failed to upload to GCS: {e}") from e


def run_zone_lookup_ingestion() -> bool:
    """
    Convenience function to run the zone lookup bronze job.
    :returns bool: True if successful, False otherwise
    """
    job = ZoneLookupIngestionJob()
    return job.run()


if __name__ == "__main__":
    success = run_zone_lookup_ingestion()
    exit(0 if success else 1)
