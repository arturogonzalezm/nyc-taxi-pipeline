"""
Spark Session Manager using Singleton Pattern.

This module provides centralized SparkSession management with specialized configuration
for MinIO/S3A integration. It addresses Hadoop 3.4.2+ compatibility issues where time
unit strings (e.g., "60s") in core-default.xml cause NumberFormatException in legacy
S3A code paths.

Design Pattern:
    - Singleton: Ensures only one SparkSession exists per application

Hadoop 3.4.2 Workaround:
    The S3A implementation has parsing issues with time unit strings introduced in
    Hadoop 3.4+. This module applies a three-layer fix:
    1. Custom core-site.xml with numeric millisecond values
    2. Java driver options set before session creation
    3. Post-initialization Hadoop configuration overrides

Thread Safety:
    Uses class-level singleton with thread-safe getOrCreate() from PySpark
"""
import os
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkSessionError(Exception):
    """
    Custom exception for SparkSession initialization failures

    :raises SparkSessionError: Thrown when SparkSession creation fails or configuration is invalid
    """
    pass


# Set HADOOP_CONF_DIR before any Spark imports to ensure custom core-site.xml is loaded
# This is the first layer of our Hadoop 3.4.2 timeout configuration fix
try:
    _project_root = Path(__file__).parent.parent.parent.parent
    _hadoop_conf_dir = _project_root / '.hadoop-conf'

    if not _hadoop_conf_dir.exists():
        raise SparkSessionError(
            f"Hadoop configuration directory not found: {_hadoop_conf_dir}\n"
            "Expected .hadoop-conf/ in project root with core-site.xml"
        )

    os.environ['HADOOP_CONF_DIR'] = str(_hadoop_conf_dir)
    logger.info(f"Set HADOOP_CONF_DIR to {_hadoop_conf_dir}")
except Exception as e:
    raise SparkSessionError(f"Failed to configure Hadoop directory: {e}") from e


class SparkSessionManager:
    """
    Singleton Spark session manager with production-ready MinIO/S3A support.

    This class manages the SparkSession lifecycle and applies specialized configuration
    to work around Hadoop 3.4.2+ compatibility issues with S3A timeout values.

    Architecture:
        - Uses PySpark's built-in thread-safe getOrCreate() for singleton behavior
        - Applies three-layer configuration fix for Hadoop 3.4.2 timeout strings
        - Validates environment variables before session creation
        - Provides proper cleanup and resource management

    Configuration Layers:
        1. HADOOP_CONF_DIR points to custom core-site.xml (set at module import)
        2. Java driver options override defaults before session creation
        3. Post-creation Hadoop configuration overrides for runtime safety

    Example:
        >>> spark = SparkSessionManager.get_session("MyETLJob", enable_s3=True)
        >>> df = spark.read.parquet("s3a://bucket/data")
        >>> # ... perform operations ...
        >>> SparkSessionManager.stop_session()  # Cleanup when done

    Thread Safety:
        The Singleton pattern is thread-safe through PySpark's getOrCreate(),
        which uses internal locking to prevent race conditions.
    """

    _instance: Optional[SparkSession] = None
    _session_config: Dict[str, Any] = {}

    @classmethod
    def _validate_s3_config(cls) -> Dict[str, str]:
        """
        Validate and retrieve S3/MinIO configuration from environment.

        :returns: Dictionary containing validated MinIO configuration
        :raises SparkSessionError: If required environment variables are missing or invalid
        """
        config = {
            'endpoint': os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            'access_key': os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            'secret_key': os.getenv("MINIO_SECRET_KEY", "minioadmin")
        }

        # Validate endpoint format
        if not config['endpoint']:
            raise SparkSessionError("MINIO_ENDPOINT cannot be empty")

        if config['endpoint'].startswith(('http://', 'https://')):
            logger.warning(
                f"MINIO_ENDPOINT should not include protocol. "
                f"Got: {config['endpoint']}. Stripping protocol."
            )
            config['endpoint'] = config['endpoint'].replace('http://', '').replace('https://', '')

        # Validate credentials
        if not config['access_key'] or not config['secret_key']:
            raise SparkSessionError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set")

        logger.info(f"MinIO configuration validated: endpoint={config['endpoint']}")
        return config

    @classmethod
    def get_session(
            cls,
            app_name: str = "NYC Taxi Data Pipeline",
            enable_s3: bool = True
    ) -> SparkSession:
        """
        Get or create Spark session with optional S3/MinIO configuration.

        This method creates a singleton SparkSession configured for optimal performance
        with MinIO object storage. It applies workarounds for Hadoop 3.4.2+ compatibility.

        :params app_name: Name of the Spark application (used in logs and UI)
        :params enable_s3: Enable S3A filesystem configuration for MinIO/S3 access
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

                # Layer 2: Set Java options to override Hadoop defaults before initialization
                # This prevents NumberFormatException from S3A code reading time unit strings
                java_opts = (
                    "-Dfs.s3a.connection.timeout=600000 "
                    "-Dfs.s3a.connection.establish.timeout=60000 "
                    "-Dfs.s3a.connection.request.timeout=600000 "
                    "-Dfs.s3a.attempts.maximum=5 "
                    "-Dfs.s3a.retry.interval=1000 "
                    "-Dfs.s3a.retry.limit=5 "
                    "-Dparquet.hadoop.vectored.io.enabled=false"
                )

                # Base Spark configuration with optimizations
                builder = SparkSession.builder \
                    .appName(app_name) \
                    .master("local[*]") \
                    .config("spark.driver.memory", "8g") \
                    .config("spark.driver.maxResultSize", "2g") \
                    .config("spark.sql.shuffle.partitions", "200") \
                    .config("spark.default.parallelism", "8") \
                    .config("spark.driver.extraJavaOptions", java_opts) \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

                # Configure for MinIO/S3A access if enabled
                if enable_s3:
                    minio_config = cls._validate_s3_config()

                    # S3A configuration for MinIO with explicit numeric timeout values
                    # These override any defaults that might use time unit strings
                    builder = builder \
                        .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_config['endpoint']}") \
                        .config("spark.hadoop.fs.s3a.access.key", minio_config['access_key']) \
                        .config("spark.hadoop.fs.s3a.secret.key", minio_config['secret_key']) \
                        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                        .config("spark.hadoop.fs.s3a.connection.timeout", "600000") \
                        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
                        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
                        .config("spark.hadoop.fs.s3a.attempts.maximum", "5") \
                        .config("spark.hadoop.fs.s3a.retry.interval", "1000") \
                        .config("spark.hadoop.fs.s3a.retry.limit", "5") \
                        .config("spark.hadoop.fs.s3a.socket.send.buffer", "65536") \
                        .config("spark.hadoop.fs.s3a.socket.recv.buffer", "65536") \
                        .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false") \
                        .config("spark.jars.packages",
                                "org.apache.hadoop:hadoop-aws:3.4.2,"
                                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                                "org.apache.hadoop:hadoop-common:3.4.2,"
                                "org.postgresql:postgresql:42.7.1")

                cls._instance = builder.getOrCreate()
                logger.info("SparkSession created successfully")

                # Layer 3: Post-creation Hadoop configuration overrides
                # This must be done AFTER SparkSession creation to override core-default.xml
                # Ensures runtime safety even if earlier layers failed
                if enable_s3:
                    cls._apply_hadoop_overrides()

                # Store configuration for diagnostics
                cls._session_config = {
                    'app_name': app_name,
                    'enable_s3': enable_s3,
                    'spark_version': cls._instance.version
                }

            except Exception as e:
                logger.error(f"Failed to create SparkSession: {e}")
                cls._instance = None
                raise SparkSessionError(f"SparkSession creation failed: {e}") from e
        else:
            logger.info(f"Returning existing SparkSession (ignoring app_name: {app_name})")

        return cls._instance

    @classmethod
    def _apply_hadoop_overrides(cls) -> None:
        """
        Apply Hadoop configuration overrides post-session creation.

        This is Layer 3 of the Hadoop 3.4.2 fix. It programmatically overrides
        any S3A timeout configurations that might have been set with time unit
        strings, replacing them with numeric millisecond values.

        :raises SparkSessionError: If Hadoop configuration access fails
        """
        if cls._instance is None:
            raise SparkSessionError("Cannot apply Hadoop overrides: no active session")

        try:
            logger.info("Applying Hadoop configuration overrides for S3A")
            hadoop_conf = cls._instance.sparkContext._jsc.hadoopConfiguration()

            # Override all S3A timeout properties with numeric millisecond values
            # This prevents NumberFormatException from "60s" string values in Hadoop 3.4+
            timeout_configs = {
                "fs.s3a.connection.timeout": "600000",
                "fs.s3a.connection.establish.timeout": "60000",
                "fs.s3a.connection.acquisition.timeout": "60000",
                "fs.s3a.connection.request.timeout": "600000",
                "fs.s3a.connection.idle.time": "60000",
                "fs.s3a.attempts.maximum": "5",
                "fs.s3a.retry.interval": "1000",
                "fs.s3a.retry.limit": "5",
                "fs.s3a.connection.maximum": "100",
                "fs.s3a.socket.send.buffer": "65536",
                "fs.s3a.socket.recv.buffer": "65536",
                "parquet.hadoop.vectored.io.enabled": "false"
            }

            for key, value in timeout_configs.items():
                hadoop_conf.set(key, value)

            logger.info(f"Applied {len(timeout_configs)} Hadoop configuration overrides")

            # Clear FileSystem cache to force re-initialization with new configuration
            # This ensures S3A filesystem picks up our numeric timeout values
            try:
                fs_class = cls._instance._jvm.org.apache.hadoop.fs.FileSystem
                fs_class.closeAll()
                logger.info("FileSystem cache cleared successfully")
            except Exception as e:
                # Non-critical: cache clear failure won't prevent S3A from working
                logger.warning(f"Failed to clear FileSystem cache: {e}")

        except Exception as e:
            logger.error(f"Failed to apply Hadoop overrides: {e}")
            raise SparkSessionError(f"Hadoop configuration override failed: {e}") from e

    @classmethod
    def stop_session(cls) -> None:
        """
        Stop and cleanup the Spark session.

        This method gracefully shuts down the SparkSession and releases all
        associated resources. After calling this method, a new session can be
        created with different configuration.

        Note:
            - Safe to call multiple times (idempotent)
            - Logs warnings if session was already stopped
            - Resets singleton state to allow new session creation
        """
        if cls._instance is not None:
            try:
                logger.info("Stopping SparkSession")
                cls._instance.stop()
                cls._instance = None
                cls._session_config = {}
                logger.info("SparkSession stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping SparkSession: {e}")
                # Still clear instance to allow retry
                cls._instance = None
                cls._session_config = {}
                raise SparkSessionError(f"Failed to stop SparkSession: {e}") from e
        else:
            logger.warning("stop_session() called but no active session exists")

    @classmethod
    def get_session_config(cls) -> Dict[str, Any]:
        """
        Get the current session configuration.

        :returns: Dictionary containing session configuration metadata
        :raises SparkSessionError: If no active session exists
        """
        if cls._instance is None:
            raise SparkSessionError("No active SparkSession")

        return cls._session_config.copy()

    @classmethod
    def is_active(cls) -> bool:
        """
        Check if a SparkSession is currently active.

        :returns: True if session exists and is active, False otherwise
        """
        return cls._instance is not None
