"""
Base Job Class (Template Pattern)
Abstract base class for all PySpark jobs with common lifecycle methods.
"""
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from typing import Optional
import logging

from .utils.spark_manager import SparkSessionManager
from .utils.config import JobConfig


class BaseSparkJob(ABC):
    """
    Template pattern implementation for PySpark jobs.
    Defines the job execution skeleton with customizable steps.
    """

    def __init__(self, job_name: str, config: Optional[JobConfig] = None):
        """
        Initialize the job.

        Args:
            job_name: Name of the job
            config: Job configuration (uses default if not provided)
        """
        self.job_name = job_name
        self.config = config or JobConfig()
        self.spark: Optional[SparkSession] = None
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging for the job"""
        logger = logging.getLogger(self.job_name)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def run(self) -> bool:
        """
        Template method defining the job execution flow.
        This is the main entry point for executing the job.

        Returns:
            True if job completed successfully, False otherwise
        """
        try:
            self.logger.info(f"Starting job: {self.job_name}")

            # Initialize Spark session
            self.spark = SparkSessionManager.get_session(
                app_name=self.job_name,
                enable_s3=self.config.minio.use_minio
            )

            # Execute job-specific logic
            self.validate_inputs()
            data = self.extract()
            transformed_data = self.transform(data)
            self.load(transformed_data)

            self.logger.info(f"Job completed successfully: {self.job_name}")
            return True

        except Exception as e:
            self.logger.error(f"Job failed: {self.job_name} - {str(e)}")
            raise

        finally:
            self.cleanup()

    @abstractmethod
    def validate_inputs(self):
        """Validate job inputs and configuration"""
        pass

    @abstractmethod
    def extract(self) -> DataFrame:
        """Extract data from source"""
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform the data"""
        pass

    @abstractmethod
    def load(self, df: DataFrame):
        """Load data to destination"""
        pass

    def cleanup(self):
        """Cleanup resources (optional override)"""
        self.logger.info(f"Cleaning up job: {self.job_name}")
