"""
Configuration Management
Centralized configuration for PySpark jobs.
"""
import os
from dataclasses import dataclass
from typing import Literal


@dataclass
class MinIOConfig:
    """MinIO/S3 configuration"""
    endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bucket: str = "nyc-taxi-pipeline"
    bronze_path: str = "bronze/trip-data"
    silver_path: str = "silver/trip-data"
    gold_path: str = "gold/trip-data"
    use_minio: bool = os.getenv("USE_MINIO", "true").lower() == "true"


@dataclass
class DataSourceConfig:
    """External data source configuration"""
    cloudfront_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"


@dataclass
class JobConfig:
    """PySpark job configuration"""
    cache_dir: str = "data/cache"

    def __init__(self):
        self.minio = MinIOConfig()
        self.source = DataSourceConfig()

    def get_s3_path(
        self,
        layer: Literal["bronze", "silver", "gold"],
        file_name: str
    ) -> str:
        """
        Get S3 path for given layer and file name.

        Args:
            layer: Data lake layer (bronze, silver, gold)
            file_name: Name of the file

        Returns:
            Full S3 path
        """
        layer_path = getattr(self.minio, f"{layer}_path")
        return f"s3a://{self.minio.bucket}/{layer_path}/{file_name}"
