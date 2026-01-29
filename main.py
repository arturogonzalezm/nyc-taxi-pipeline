import os
import argparse
import requests
from typing import Literal, Optional
from pathlib import Path
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

# Set JAVA_HOME to Java 17 for Spark 4.x compatibility
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "nyc-taxi-pipeline"
MINIO_BRONZE_PATH = "bronze/trip-data"
USE_MINIO = os.getenv("USE_MINIO", "true").lower() == "true"

# CloudFront URL (fallback when MinIO doesn't have data)
CLOUDFRONT_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


# Strategy Pattern: Different data loading strategies
class DataLoaderStrategy(ABC):
    """Abstract base class for data loading strategies"""

    @abstractmethod
    def load(self, spark: SparkSession, year: int, month: int, cache_dir: str) -> DataFrame:
        """Load data for given year and month"""
        pass


class YellowTaxiLoader(DataLoaderStrategy):
    """Strategy for loading Yellow Taxi data"""

    def load(self, spark: SparkSession, year: int, month: int, cache_dir: str = "data/cache") -> DataFrame:
        return self._download_and_load(spark, "yellow", year, month, cache_dir)

    def _download_and_load(self, spark: SparkSession, taxi_type: str, year: int, month: int, cache_dir: str) -> DataFrame:
        month_str = f"{month:02d}"
        file_name = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"

        if USE_MINIO:
            # Use MinIO bronze layer
            s3_path = f"s3a://{MINIO_BUCKET}/{MINIO_BRONZE_PATH}/{file_name}"
            print(f"Loading {taxi_type} taxi data from MinIO bronze: {s3_path}...")

            try:
                return spark.read.parquet(s3_path)
            except Exception as e:
                print(f"Failed to load from MinIO: {e}")
                print("Falling back to download and upload to MinIO bronze...")
                return self._download_upload_and_load(spark, taxi_type, year, month, cache_dir, file_name)
        else:
            # Use CloudFront
            url = f"{CLOUDFRONT_URL}/{file_name}"
            return self._download_from_cloudfront(spark, taxi_type, url, cache_dir, file_name)

    def _download_from_cloudfront(self, spark: SparkSession, taxi_type: str, url: str, cache_dir: str, file_name: str) -> DataFrame:
        cache_path = Path(cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        local_file = cache_path / file_name

        if not local_file.exists():
            print(f"Downloading {taxi_type} taxi data from {url}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(local_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded to {local_file}")
        else:
            print(f"Using cached file: {local_file}")

        return spark.read.parquet(str(local_file))

    def _download_upload_and_load(self, spark: SparkSession, taxi_type: str, year: int, month: int, cache_dir: str, file_name: str) -> DataFrame:
        # Download from CloudFront
        url = f"{CLOUDFRONT_URL}/{file_name}"
        cache_path = Path(cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        local_file = cache_path / file_name

        if not local_file.exists():
            print(f"Downloading {taxi_type} taxi data from {url}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(local_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded to {local_file}")

        # Upload to MinIO bronze layer
        print(f"Uploading to MinIO bronze: {MINIO_BUCKET}/{MINIO_BRONZE_PATH}/{file_name}...")
        s3_path = f"s3a://{MINIO_BUCKET}/{MINIO_BRONZE_PATH}/{file_name}"
        df = spark.read.parquet(str(local_file))
        df.write.mode("overwrite").parquet(s3_path)
        print(f"Uploaded to MinIO")

        return df


class GreenTaxiLoader(DataLoaderStrategy):
    """Strategy for loading Green Taxi data"""

    def load(self, spark: SparkSession, year: int, month: int, cache_dir: str = "data/cache") -> DataFrame:
        return self._download_and_load(spark, "green", year, month, cache_dir)

    def _download_and_load(self, spark: SparkSession, taxi_type: str, year: int, month: int, cache_dir: str) -> DataFrame:
        month_str = f"{month:02d}"
        file_name = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"

        if USE_MINIO:
            # Use MinIO bronze layer
            s3_path = f"s3a://{MINIO_BUCKET}/{MINIO_BRONZE_PATH}/{file_name}"
            print(f"Loading {taxi_type} taxi data from MinIO bronze: {s3_path}...")

            try:
                return spark.read.parquet(s3_path)
            except Exception as e:
                print(f"Failed to load from MinIO: {e}")
                print("Falling back to download and upload to MinIO bronze...")
                return self._download_upload_and_load(spark, taxi_type, year, month, cache_dir, file_name)
        else:
            # Use CloudFront
            url = f"{CLOUDFRONT_URL}/{file_name}"
            return self._download_from_cloudfront(spark, taxi_type, url, cache_dir, file_name)

    def _download_from_cloudfront(self, spark: SparkSession, taxi_type: str, url: str, cache_dir: str, file_name: str) -> DataFrame:
        cache_path = Path(cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        local_file = cache_path / file_name

        if not local_file.exists():
            print(f"Downloading {taxi_type} taxi data from {url}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(local_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded to {local_file}")
        else:
            print(f"Using cached file: {local_file}")

        return spark.read.parquet(str(local_file))

    def _download_upload_and_load(self, spark: SparkSession, taxi_type: str, year: int, month: int, cache_dir: str, file_name: str) -> DataFrame:
        # Download from CloudFront
        url = f"{CLOUDFRONT_URL}/{file_name}"
        cache_path = Path(cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        local_file = cache_path / file_name

        if not local_file.exists():
            print(f"Downloading {taxi_type} taxi data from {url}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(local_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded to {local_file}")

        # Upload to MinIO bronze layer
        print(f"Uploading to MinIO bronze: {MINIO_BUCKET}/{MINIO_BRONZE_PATH}/{file_name}...")
        s3_path = f"s3a://{MINIO_BUCKET}/{MINIO_BRONZE_PATH}/{file_name}"
        df = spark.read.parquet(str(local_file))
        df.write.mode("overwrite").parquet(s3_path)
        print(f"Uploaded to MinIO")

        return df


# Factory Pattern: Create appropriate data loader
class TaxiDataLoaderFactory:
    """Factory for creating taxi data loaders"""

    _loaders = {
        "yellow": YellowTaxiLoader,
        "green": GreenTaxiLoader
    }

    @classmethod
    def create_loader(cls, taxi_type: Literal["yellow", "green"]) -> DataLoaderStrategy:
        """Create and return appropriate data loader"""
        loader_class = cls._loaders.get(taxi_type)
        if not loader_class:
            raise ValueError(f"Unknown taxi type: {taxi_type}. Supported types: {list(cls._loaders.keys())}")
        return loader_class()


# Singleton Pattern: Spark session manager
class SparkSessionManager:
    """Singleton to manage Spark session"""

    _instance: Optional[SparkSession] = None

    @classmethod
    def get_session(cls, app_name: str = "NYC Taxi Data Pipeline") -> SparkSession:
        """Get or create Spark session"""
        if cls._instance is None:
            builder = SparkSession.builder \
                .appName(app_name) \
                .master("local[*]") \
                .config("spark.driver.memory", "4g")

            # Configure for MinIO S3 access if enabled
            if USE_MINIO:
                builder = builder \
                    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
                    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
                    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")

            cls._instance = builder.getOrCreate()
        return cls._instance

    @classmethod
    def stop_session(cls):
        """Stop Spark session"""
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None


# Facade Pattern: Simple interface for data loading
class TaxiDataPipeline:
    """Facade for taxi data pipeline operations"""

    def __init__(self, taxi_type: Literal["yellow", "green"], year: int, month: int):
        self.taxi_type = taxi_type
        self.year = year
        self.month = month
        self.spark = SparkSessionManager.get_session()
        self.loader = TaxiDataLoaderFactory.create_loader(taxi_type)

    def load_data(self, cache_dir: str = "data/cache") -> DataFrame:
        """Load taxi data"""
        print(f"\nRetrieving NYC {self.taxi_type.title()} Taxi trip data for {self.year}-{self.month:02d}...")
        return self.loader.load(self.spark, self.year, self.month, cache_dir)

    def show_summary(self, df: DataFrame):
        """Display data summary"""
        print(f"\n✓ Retrieved {df.count():,} records")
        print(f"\nSchema:")
        df.printSchema()
        print(f"\nSample data:")
        df.show(5, truncate=False)

    def save_data(self, df: DataFrame, output_path: Optional[str] = None):
        """Save data to parquet"""
        if output_path is None:
            output_path = f"data/{self.taxi_type}_taxi_{self.year}_{self.month:02d}.parquet"

        print(f"\nSaving data to {output_path}...")
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Data saved successfully")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="NYC Taxi Data Pipeline")
    parser.add_argument("--taxi-type", type=str, choices=["yellow", "green"], required=True,
                        help="Type of taxi data (yellow or green)")
    parser.add_argument("--year", type=int, required=True,
                        help="Year (e.g., 2024)")
    parser.add_argument("--month", type=int, required=True,
                        help="Month (1-12)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output path for parquet file (optional)")
    parser.add_argument("--cache-dir", type=str, default="data/cache",
                        help="Cache directory for downloaded files")

    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_arguments()

    # Validate month
    if not 1 <= args.month <= 12:
        print(f"Error: Month must be between 1 and 12, got {args.month}")
        return

    try:
        # Create pipeline using Facade pattern
        pipeline = TaxiDataPipeline(args.taxi_type, args.year, args.month)

        # Load data using Strategy pattern (via Factory)
        df = pipeline.load_data(args.cache_dir)

        # Show summary
        pipeline.show_summary(df)

        # Save data
        pipeline.save_data(df, args.output)

    except Exception as e:
        print(f"\nError: {e}")
        raise
    finally:
        # Clean up Spark session (Singleton)
        SparkSessionManager.stop_session()


if __name__ == "__main__":
    main()
