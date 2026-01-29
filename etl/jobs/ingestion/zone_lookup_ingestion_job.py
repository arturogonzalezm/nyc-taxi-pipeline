"""
NYC Taxi Zone Lookup Ingestion Job
Ingests taxi zone lookup reference data from NYC TLC to MinIO.
"""
import os
import requests
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from minio import Minio
from minio.error import S3Error

from ..base_job import BaseSparkJob
from ..utils.config import JobConfig


class ZoneLookupIngestionJob(BaseSparkJob):
    """
    Ingestion job for NYC Taxi Zone Lookup reference data.
    Downloads zone lookup CSV and stores it in MinIO misc layer.
    """

    def __init__(self, config: JobConfig = None):
        """
        Initialize the zone lookup ingestion job.

        Args:
            config: Job configuration
        """
        super().__init__(
            job_name="ZoneLookupIngestion",
            config=config
        )
        self.file_name = "taxi_zone_lookup.csv"
        self.source_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    def validate_inputs(self):
        """Validate job inputs"""
        self.logger.info(f"Validating zone lookup ingestion from {self.source_url}")

    def extract(self) -> DataFrame:
        """
        Extract zone lookup data from NYC TLC.
        Downloads CSV file and loads into Spark DataFrame.
        """
        return self._extract_from_source()

    def _extract_from_source(self) -> DataFrame:
        """Download and load zone lookup CSV from NYC TLC"""
        cache_path = Path(self.config.cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        local_file = cache_path / self.file_name

        if not local_file.exists():
            self.logger.info(f"Downloading zone lookup from: {self.source_url}")

            response = requests.get(self.source_url, stream=True)
            response.raise_for_status()

            # Download CSV file
            with open(local_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            self.logger.info(f"Downloaded to {local_file}")
        else:
            self.logger.info(f"Using cached file: {local_file}")

        # Read CSV with header
        df = self.spark.read.csv(str(local_file), header=True, inferSchema=True)

        record_count = df.count()
        self.logger.info(f"Loaded {record_count:,} zone records")

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Validate the zone lookup data.
        No transformations - just validation for reference data.
        """
        self.logger.info("Validating zone lookup data")
        self.logger.info(f"Schema: {df.schema}")

        # Display sample of data
        self.logger.info("Sample data:")
        df.show(5, truncate=False)

        # Validate required columns exist
        required_columns = ["LocationID", "Borough", "Zone", "service_zone"]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Check for null LocationIDs (primary key)
        null_location_count = df.filter(F.col("LocationID").isNull()).count()
        if null_location_count > 0:
            self.logger.warning(f"Found {null_location_count} records with null LocationID")

        record_count = df.count()
        self.logger.info(f"Zone lookup validation complete: {record_count:,} records")

        # Return original dataframe without modifications
        return df

    def load(self, df: DataFrame):
        """
        Load zone lookup data to MinIO misc layer as raw CSV file.
        Uploads the downloaded CSV file directly to preserve original format.
        """
        record_count = df.count()
        self.logger.info(f"Validated {record_count:,} zone records")

        if self.config.minio.use_minio:
            # Get the local cached file
            cache_path = Path(self.config.cache_dir)
            local_file = cache_path / self.file_name

            if not local_file.exists():
                raise FileNotFoundError(f"Local file not found: {local_file}")

            # Upload raw CSV file directly to MinIO
            try:
                # Initialize MinIO client
                endpoint = self.config.minio.endpoint
                # Remove http:// or https:// prefix if present
                endpoint = endpoint.replace("http://", "").replace("https://", "")

                minio_client = Minio(
                    endpoint,
                    access_key=self.config.minio.access_key,
                    secret_key=self.config.minio.secret_key,
                    secure=False  # Set to True if using HTTPS
                )

                # Ensure bucket exists
                bucket_name = self.config.minio.bucket
                if not minio_client.bucket_exists(bucket_name):
                    self.logger.info(f"Creating bucket: {bucket_name}")
                    minio_client.make_bucket(bucket_name)

                # Upload file to misc directory
                object_name = f"misc/{self.file_name}"
                self.logger.info(f"Uploading {local_file} to {bucket_name}/{object_name}")

                minio_client.fput_object(
                    bucket_name,
                    object_name,
                    str(local_file),
                    content_type="text/csv"
                )

                self.logger.info(f"Successfully uploaded {self.file_name} to MinIO")
                self.logger.info(f"MinIO path: s3a://{bucket_name}/{object_name}")

            except S3Error as e:
                self.logger.error(f"MinIO error: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Error uploading to MinIO: {e}")
                raise
        else:
            # If not using MinIO, just keep it in cache
            self.logger.info(f"File available in cache: {self.config.cache_dir}/{self.file_name}")
            self.logger.info("MinIO upload skipped (USE_MINIO=false)")


def run_zone_lookup_ingestion() -> bool:
    """
    Convenience function to run the zone lookup ingestion job.

    Returns:
        True if successful, False otherwise
    """
    job = ZoneLookupIngestionJob()
    return job.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC Taxi Zone Lookup Ingestion Job")
    args = parser.parse_args()

    success = run_zone_lookup_ingestion()
    exit(0 if success else 1)
