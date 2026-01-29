"""
NYC Taxi Data Ingestion Job
Ingests taxi trip data from CloudFront to MinIO bronze layer.
"""
import os
import requests
from pathlib import Path
from typing import Literal
from pyspark.sql import DataFrame

from ..base_job import BaseSparkJob
from ..utils.config import JobConfig


class TaxiIngestionJob(BaseSparkJob):
    """
    Ingestion job for NYC Taxi trip data.
    Downloads data from CloudFront and stores it in MinIO bronze layer.
    """

    def __init__(
        self,
        taxi_type: Literal["yellow", "green"],
        year: int,
        month: int,
        config: JobConfig = None
    ):
        """
        Initialize the ingestion job.

        Args:
            taxi_type: Type of taxi (yellow or green)
            year: Year of the data
            month: Month of the data
            config: Job configuration
        """
        super().__init__(
            job_name=f"TaxiIngestion_{taxi_type}_{year}_{month:02d}",
            config=config
        )
        self.taxi_type = taxi_type
        self.year = year
        self.month = month
        self.file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"

    def validate_inputs(self):
        """Validate job inputs"""
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
        First attempts to load from MinIO, falls back to CloudFront download.
        """
        if self.config.minio.use_minio:
            try:
                return self._extract_from_minio()
            except Exception as e:
                self.logger.warning(f"Failed to load from MinIO: {e}")
                self.logger.info("Falling back to CloudFront download...")
                return self._extract_from_cloudfront()
        else:
            return self._extract_from_cloudfront()

    def _extract_from_minio(self) -> DataFrame:
        """Load data from MinIO bronze layer"""
        s3_path = self.config.get_s3_path("bronze", self.file_name)
        self.logger.info(f"Loading from MinIO: {s3_path}")
        return self.spark.read.parquet(s3_path)

    def _extract_from_cloudfront(self) -> DataFrame:
        """Download and load data from CloudFront"""
        url = f"{self.config.source.cloudfront_url}/{self.file_name}"
        cache_path = Path(self.config.cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        local_file = cache_path / self.file_name

        if not local_file.exists():
            self.logger.info(f"Downloading from CloudFront: {url}")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(local_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            self.logger.info(f"Downloaded to {local_file}")
        else:
            self.logger.info(f"Using cached file: {local_file}")

        return self.spark.read.parquet(str(local_file))

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform the data.
        For bronze layer, we typically do minimal transformation.
        """
        self.logger.info(f"Processing {df.count():,} records")

        # Add metadata columns for tracking
        from pyspark.sql import functions as F

        df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
               .withColumn("source_file", F.lit(self.file_name)) \
               .withColumn("taxi_type", F.lit(self.taxi_type)) \
               .withColumn("data_year", F.lit(self.year)) \
               .withColumn("data_month", F.lit(self.month))

        return df

    def load(self, df: DataFrame):
        """Load data to MinIO bronze layer"""
        if self.config.minio.use_minio:
            s3_path = self.config.get_s3_path("bronze", self.file_name)
            self.logger.info(f"Writing to MinIO bronze layer: {s3_path}")

            df.write.mode("overwrite").parquet(s3_path)

            self.logger.info(f"Successfully loaded {df.count():,} records to bronze layer")
        else:
            # If not using MinIO, save locally
            output_path = f"{self.config.cache_dir}/output/{self.file_name}"
            self.logger.info(f"Writing to local path: {output_path}")
            df.write.mode("overwrite").parquet(output_path)


def run_ingestion(
    taxi_type: Literal["yellow", "green"],
    year: int,
    month: int
) -> bool:
    """
    Convenience function to run the ingestion job.

    Args:
        taxi_type: Type of taxi (yellow or green)
        year: Year of the data
        month: Month of the data

    Returns:
        True if successful, False otherwise
    """
    job = TaxiIngestionJob(taxi_type, year, month)
    return job.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC Taxi Data Ingestion Job")
    parser.add_argument("--taxi-type", type=str, choices=["yellow", "green"], required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)

    args = parser.parse_args()

    success = run_ingestion(args.taxi_type, args.year, args.month)
    exit(0 if success else 1)
