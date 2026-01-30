"""
Base Job Class implementing Template Method and Strategy patterns.

This module provides an abstract base class for all PySpark ETL jobs, implementing
the Template Method pattern for job execution flow and Strategy pattern for
configurable components.

Design Patterns:
    - Template Method: Defines the skeleton of job execution
    - Strategy: Allows different extraction, gold, and loading strategies
    - Singleton: Uses singleton SparkSession and configuration
"""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict, Any
import logging
import time
from datetime import datetime

from .utils.spark_manager import SparkSessionManager
from .utils.config import JobConfig


class JobExecutionError(Exception):
    """
    Custom exception for job execution failures
    """

    pass


class BaseSparkJob(ABC):
    """
    Abstract base class for PySpark ETL jobs using Template Method pattern.

    This class provides a standardised framework for ETL jobs with:
    - Consistent execution flow (validate -> extract -> transform -> load)
    - Centralised error handling and logging
    - Resource management and cleanup
    - Job metrics and monitoring

    Best Practices:
        - Thread-safe Spark session management
        - Proper resource cleanup in finally block
        - Structured logging with timestamps
        - Exception handling with context
        - Job metrics tracking

    Example:
        >>> class MyJob(BaseSparkJob):
        ...     def validate_inputs(self):
        ...         # Validation logic
        ...         pass
        ...     def extract(self) -> DataFrame:
        ...         return self.spark.read.parquet("path")
        ...     def transform(self, df: DataFrame) -> DataFrame:
        ...         return df.filter("column > 0")
        ...     def load(self, df: DataFrame):
        ...         df.write.parquet("output")
        >>> job = MyJob("my_job")
        >>> success = job.run()
    """

    def __init__(self, job_name: str, config: Optional[JobConfig] = None):
        """
        Initialise the ETL job.
        :params job_name: Unique identifier for the job (used for logging and monitoring)
        :params config: Job configuration instance (uses singleton default if not provided)

        Raises:
            ValueError: If job_name is empty or invalid
        """
        if not job_name or not job_name.strip():
            raise ValueError("job_name cannot be empty")

        self.job_name = job_name.strip()
        self.config = config or JobConfig()
        self.spark: Optional[SparkSession] = None
        self.logger = self._setup_logger()
        self._metrics: Dict[str, Any] = {}
        self._start_time: Optional[float] = None

    def _setup_logger(self) -> logging.Logger:
        """
        Configure structured logging for the job.
        :returns: Configured logger instance

        Note:
            - Uses INFO level by default
            - Prevents duplicate handlers
            - Includes timestamp and job name in format
        """
        logger = logging.getLogger(self.job_name)
        logger.setLevel(logging.INFO)

        # Prevent duplicate handlers
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    @contextmanager
    def _track_metrics(self, step: str):
        """
        Context manager for tracking execution metrics per step.
        :params step: Name of the execution step (e.g., 'extract', 'transform')
        """
        step_start = time.time()
        self.logger.info(f"Starting step: {step}")
        try:
            yield
        finally:
            duration = time.time() - step_start
            self._metrics[f"{step}_duration_seconds"] = round(duration, 2)
            self.logger.info(f"Completed step: {step} (took {duration:.2f}s)")

    def run(self) -> bool:
        """
        Execute the ETL job following the Template Method pattern.

        This method defines the invariant job execution flow:
        1. Initialise Spark session
        2. Validate inputs
        3. Extract data
        4. Transform data
        5. Load data
        6. Cleanup resources

        :returns: True if job completed successfully, False otherwise
        :raises JobExecutionError: If any step fails with context about the failure
        """
        self._start_time = time.time()
        self._metrics["job_name"] = self.job_name
        self._metrics["start_time"] = datetime.now().isoformat()
        self._metrics["status"] = "FAILED"  # Pessimistic default

        try:
            self.logger.info("=" * 80)
            self.logger.info(f"Starting job: {self.job_name}")
            self.logger.info("=" * 80)

            # Initialise Spark session
            with self._track_metrics("initialisation"):
                self.spark = SparkSessionManager.get_session(
                    app_name=self.job_name, enable_s3=self.config.minio.use_minio
                )

            # Execute ETL pipeline
            with self._track_metrics("validation"):
                self.validate_inputs()

            with self._track_metrics("extract"):
                data = self.extract()
                if data is None:
                    raise JobExecutionError("Extract step returned None")

            with self._track_metrics("transform"):
                transformed_data = self.transform(data)
                if transformed_data is None:
                    raise JobExecutionError("Transform step returned None")

            with self._track_metrics("load"):
                self.load(transformed_data)

            # Job succeeded
            self._metrics["status"] = "SUCCESS"
            total_duration = time.time() - self._start_time
            self._metrics["total_duration_seconds"] = round(total_duration, 2)

            self.logger.info("=" * 80)
            self.logger.info(f"Job completed successfully: {self.job_name}")
            self.logger.info(f"Total duration: {total_duration:.2f} seconds")
            self.logger.info(f"Metrics: {self._metrics}")
            self.logger.info("=" * 80)

            return True

        except Exception as e:
            self._metrics["error_message"] = str(e)
            self._metrics["error_type"] = type(e).__name__

            self.logger.error("=" * 80)
            self.logger.error(f"Job failed: {self.job_name}")
            self.logger.error(f"Error: {str(e)}")
            self.logger.error(f"Metrics: {self._metrics}")
            self.logger.error("=" * 80)

            # Re-raise as JobExecutionError for consistent error handling
            if isinstance(e, JobExecutionError):
                raise
            raise JobExecutionError(f"Job {self.job_name} failed: {str(e)}") from e

        finally:
            # Always cleanup resources
            self.cleanup()

    @abstractmethod
    def validate_inputs(self) -> None:
        """
        Validate job inputs and configuration.

        This method should check:
        - Required parameters are present
        - Input data sources are accessible
        - Configuration values are valid

        :raises ValueError: If validation fails
        :raises JobExecutionError: If critical validation errors occur
        """
        pass

    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extract data from source systems.
        :returns: DataFrame containing extracted data
        :raises JobExecutionError: If extraction fails
        """
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform the extracted data.
        :params df: Input DataFrame from extract step
        :returns: Transformed DataFrame
        :raises JobExecutionError: If gold fails
        """
        pass

    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """
        Load transformed data to destination.
        :params df: Transformed DataFrame from transform step
        :raises JobExecutionError: If load fails
        """
        pass

    def cleanup(self) -> None:
        """
        Cleanup resources after job execution.

        Override this method to add custom cleanup logic (e.g., closing connections,
        removing temporary files). This method is always called, even if the job fails.

        Note:
            - Called in finally block to ensure execution
            - Should not raise exceptions
            - Should be idempotent (safe to call multiple times)
        """
        try:
            self.logger.info(f"Cleaning up job: {self.job_name}")
            # Base cleanup logic can be added here
        except Exception as e:
            self.logger.warning(f"Cleanup warning: {str(e)}")

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get job execution metrics.
        :returns: Dictionary containing job metrics (durations, status, etc.)
        """
        return self._metrics.copy()
