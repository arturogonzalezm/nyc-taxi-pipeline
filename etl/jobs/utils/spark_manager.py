"""
Spark Session Manager using Singleton Pattern.

This module provides centralized SparkSession management with specialized configuration
for Google Cloud Storage (GCS) integration.

Design Pattern:
    - Singleton: Ensures only one SparkSession exists per application

Thread Safety:
    Uses class-level singleton with thread-safe getOrCreate() from PySpark
"""

import os
import logging
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession

from etl.jobs.utils.terraform_config import get_gcp_config_with_fallback

logger = logging.getLogger(__name__)


class SparkSessionError(Exception):
    """
    Custom exception for SparkSession initialization failures

    :raises SparkSessionError: Thrown when SparkSession creation fails or configuration is invalid
    """

    pass


class SparkSessionManager:
    """
    Singleton Spark session manager with production-ready GCS support.

    This class manages the SparkSession lifecycle and applies specialized configuration
    for Google Cloud Storage integration.

    Architecture:
        - Uses PySpark's built-in thread-safe getOrCreate() for singleton behavior
        - Validates environment variables before session creation
        - Provides proper cleanup and resource management
        - Configures GCS via the GCS connector

    Example:
        >>> spark = SparkSessionManager.get_session("MyETLJob")
        >>> df = spark.read.parquet("gs://bucket/data")
        >>> # ... perform operations ...
        >>> SparkSessionManager.stop_session()  # Cleanup when done

    Thread Safety:
        The Singleton pattern is thread-safe through PySpark's getOrCreate(),
        which uses internal locking to prevent race conditions.
    """

    _instance: Optional[SparkSession] = None
    _session_config: Dict[str, Any] = {}

    @classmethod
    def get_session(cls, app_name: str = "NYC Taxi Data Pipeline") -> SparkSession:
        """
        Get or create Spark session with GCS configuration.

        This method creates a singleton SparkSession configured for optimal performance
        with Google Cloud Storage.

        :params app_name: Name of the Spark application (used in logs and UI)
        :returns SparkSession: Configured SparkSession instance (singleton)
        :raises SparkSessionError: If session creation fails or configuration is invalid
        :raises ValueError: If app_name is empty

        Note:
            - First call creates the session with specified configuration
            - Subsequent calls return existing session (configuration changes ignored)
            - Call stop_session() to destroy session and allow reconfiguration
        """
        if not app_name or not app_name.strip():
            raise ValueError("app_name cannot be empty")

        app_name = app_name.strip()

        if cls._instance is None:
            try:
                logger.info(f"Creating new SparkSession: {app_name}")

                # Get GCP project ID with fallback to terraform.tfvars
                gcs_project, _ = get_gcp_config_with_fallback()
                gcs_keyfile = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
                logger.info(f"Configuring Spark for GCS with project: {gcs_project}")
                if gcs_keyfile:
                    logger.info(f"Using service account keyfile: {gcs_keyfile}")

                # Base Spark configuration with optimizations
                builder = (
                    SparkSession.builder.appName(app_name)
                    .master("local[*]")
                    .config("spark.driver.memory", "8g")
                    .config("spark.driver.maxResultSize", "2g")
                    .config("spark.sql.shuffle.partitions", "200")
                    .config("spark.default.parallelism", "8")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    # GCS configuration using the GCS connector
                    .config(
                        "spark.hadoop.fs.gs.impl",
                        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                    )
                    .config(
                        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
                    )
                    .config("spark.hadoop.fs.gs.project.id", gcs_project)
                    .config(
                        "spark.hadoop.fs.gs.auth.type",
                        "SERVICE_ACCOUNT_JSON_KEYFILE" if gcs_keyfile else "APPLICATION_DEFAULT",
                    )
                    .config(
                        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                        gcs_keyfile,
                    )
                    .config(
                        "spark.jars.packages",
                        "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.18,"
                        "org.postgresql:postgresql:42.7.1",
                    )
                )

                cls._instance = builder.getOrCreate()
                logger.info("SparkSession created successfully")

                # Store configuration for diagnostics
                cls._session_config = {
                    "app_name": app_name,
                    "gcs_project": gcs_project,
                    "spark_version": cls._instance.version,
                }

            except Exception as e:
                logger.error(f"Failed to create SparkSession: {e}")
                raise SparkSessionError(f"SparkSession creation failed: {e}") from e

        return cls._instance

    @classmethod
    def stop_session(cls):
        """
        Stop and cleanup the SparkSession.

        This method should be called when the application is done using Spark
        to properly release resources.

        Note:
            After calling this method, get_session() will create a new session.
        """
        if cls._instance is not None:
            logger.info("Stopping SparkSession")
            try:
                cls._instance.stop()
            except Exception as e:
                logger.warning(f"Error stopping SparkSession: {e}")
            finally:
                cls._instance = None
                cls._session_config = {}

    @classmethod
    def get_session_info(cls) -> Dict[str, Any]:
        """
        Get information about the current session.

        :returns: Dictionary with session configuration and status
        """
        if cls._instance is None:
            return {"status": "not_initialized"}

        return {
            "status": "active",
            **cls._session_config,
        }
