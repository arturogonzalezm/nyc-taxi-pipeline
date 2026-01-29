"""
Spark Session Manager (Singleton Pattern)
Manages Spark session lifecycle with configuration for MinIO/S3A support.
"""
import os
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession

# Set HADOOP_CONF_DIR to our custom config directory before importing Spark
# This ensures Hadoop reads our core-site.xml with numeric timeout values
_project_root = Path(__file__).parent.parent.parent.parent
os.environ['HADOOP_CONF_DIR'] = str(_project_root / '.hadoop-conf')


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
            # Set Java options to override Hadoop defaults before initialization
            java_opts = (
                "-Dfs.s3a.connection.timeout=60000 "
                "-Dfs.s3a.connection.establish.timeout=60000 "
                "-Dfs.s3a.attempts.maximum=3 "
                "-Dfs.s3a.retry.interval=500 "
                "-Dfs.s3a.retry.limit=3"
            )

            builder = SparkSession.builder \
                .appName(app_name) \
                .master("local[*]") \
                .config("spark.driver.memory", "4g") \
                .config("spark.driver.extraJavaOptions", java_opts) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

            # Configure for MinIO/S3A access if enabled
            if enable_s3:
                minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
                minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
                minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

                # S3A configuration for MinIO with explicit timeout values
                builder = builder \
                    .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
                    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
                    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
                    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
                    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
                    .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
                    .config("spark.hadoop.fs.s3a.retry.interval", "500") \
                    .config("spark.hadoop.fs.s3a.retry.limit", "3") \
                    .config("spark.hadoop.fs.s3a.socket.send.buffer", "8192") \
                    .config("spark.hadoop.fs.s3a.socket.recv.buffer", "8192") \
                    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hadoop:hadoop-common:3.4.2")

            cls._instance = builder.getOrCreate()

            # Override any system-level Hadoop configs that may have string time values
            # This must be done AFTER SparkSession creation to override core-default.xml
            if enable_s3:
                hadoop_conf = cls._instance.sparkContext._jsc.hadoopConfiguration()

                # Set all S3A timeout properties to numeric millisecond values
                # This overrides the "60s" string values from Hadoop 3.4+ core-default.xml
                hadoop_conf.set("fs.s3a.connection.timeout", "60000")
                hadoop_conf.set("fs.s3a.connection.establish.timeout", "60000")
                hadoop_conf.set("fs.s3a.connection.acquisition.timeout", "60000")
                hadoop_conf.set("fs.s3a.connection.request.timeout", "60000")
                hadoop_conf.set("fs.s3a.connection.idle.time", "60000")
                hadoop_conf.set("fs.s3a.attempts.maximum", "3")
                hadoop_conf.set("fs.s3a.retry.interval", "500")
                hadoop_conf.set("fs.s3a.retry.limit", "3")
                hadoop_conf.set("fs.s3a.connection.maximum", "100")
                hadoop_conf.set("fs.s3a.socket.send.buffer", "8192")
                hadoop_conf.set("fs.s3a.socket.recv.buffer", "8192")

                # Clear FileSystem cache to force re-initialization with new config
                try:
                    fs_class = cls._instance._jvm.org.apache.hadoop.fs.FileSystem
                    fs_class.closeAll()
                except Exception as e:
                    # If closeAll fails, it's not critical - continue
                    pass

        return cls._instance

    @classmethod
    def stop_session(cls):
        """Stop and cleanup Spark session"""
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None
