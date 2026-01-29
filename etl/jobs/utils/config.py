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
    bronze_path: str = "bronze/nyc_taxi"
    silver_path: str = "silver/nyc_taxi"
    gold_path: str = "gold/nyc_taxi"
    use_minio: bool = os.getenv("USE_MINIO", "true").lower() == "true"


@dataclass
class DataSourceConfig:
    """External data source configuration"""
    base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"


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
        taxi_type: str = None,
        file_name: str = None
    ) -> str:
        """
        Get S3 path for given layer and taxi type.

        Args:
            layer: Data lake layer (bronze, silver, gold)
            taxi_type: Type of taxi (yellow, green) - optional
            file_name: Name of the file - optional (deprecated, kept for backward compatibility)

        Returns:
            Full S3 path
        """
        layer_path = getattr(self.minio, f"{layer}_path")
        if taxi_type:
            return f"s3a://{self.minio.bucket}/{layer_path}/{taxi_type}"
        elif file_name:
            # Backward compatibility
            return f"s3a://{self.minio.bucket}/{layer_path}/{file_name}"
        else:
            return f"s3a://{self.minio.bucket}/{layer_path}"
