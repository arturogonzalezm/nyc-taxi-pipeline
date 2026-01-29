"""
Spark Session Manager (Singleton Pattern)
Manages Spark session lifecycle with configuration for MinIO/S3A support.
"""
import os
from typing import Optional
from pyspark.sql import SparkSession


class SparkSessionManager:
    """
    Singleton pattern implementation for Spark session management.
    Ensures only one Spark session exists throughout the application lifecycle.
    """

    _instance: Optional[SparkSession] = None

    @classmethod
    def get_session(
        cls,
        app_name: str = "NYC Taxi Data Pipeline",
        enable_s3: bool = True
    ) -> SparkSession:
        """
        Get or create Spark session with optional S3/MinIO configuration.

        Args:
            app_name: Name of the Spark application
            enable_s3: Enable S3A configuration for MinIO

        Returns:
            SparkSession instance
        """
        if cls._instance is None:
            builder = SparkSession.builder \
                .appName(app_name) \
                .master("local[*]") \
                .config("spark.driver.memory", "4g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

            # Configure for MinIO/S3A access if enabled
            if enable_s3:
                minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
                minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
                minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

                builder = builder \
                    .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
                    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
                    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")

            cls._instance = builder.getOrCreate()

        return cls._instance

    @classmethod
    def stop_session(cls):
        """Stop and cleanup Spark session"""
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None
