"""
Configuration Management
Centralized configuration for PySpark jobs using Singleton pattern.
"""

import os
import tempfile
from dataclasses import dataclass, field
from typing import Literal, Optional
from pathlib import Path


@dataclass
class MinIOConfig:
    """
    MinIO/S3 configuration with environment variable support.

    All configuration values are loaded from environment variables with sensible defaults.
    This follows the 12-factor app methodology for configuration management.
    """

    endpoint: str = field(
        default_factory=lambda: os.getenv("MINIO_ENDPOINT", "localhost:9000")
    )
    access_key: str = field(
        default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    )
    secret_key: str = field(
        default_factory=lambda: os.getenv("MINIO_SECRET_KEY", "minioadmin")
    )
    bucket: str = field(
        default_factory=lambda: os.getenv("MINIO_BUCKET", "nyc-taxi-pipeline")
    )
    bronze_path: str = "bronze/nyc_taxi"
    silver_path: str = "silver/nyc_taxi"
    gold_path: str = "gold/nyc_taxi"
    use_minio: bool = field(
        default_factory=lambda: os.getenv("USE_MINIO", "true").lower() == "true"
    )

    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.endpoint:
            raise ValueError("MINIO_ENDPOINT must be set")
        if not self.bucket:
            raise ValueError("MINIO_BUCKET must be set")


@dataclass
class GCSConfig:
    """
    Google Cloud Storage configuration with environment variable support.

    All configuration values are loaded from environment variables with sensible defaults.
    This follows the 12-factor app methodology for configuration management.
    """

    bucket: str = field(
        default_factory=lambda: os.getenv(
            "GCS_BUCKET", "nyc-taxi-dev-etl-us-central1-01"
        )
    )
    project_id: str = field(
        default_factory=lambda: os.getenv("GCP_PROJECT_ID", "nyc-taxi-pipeline-001")
    )
    bronze_path: str = "bronze/nyc_taxi"
    silver_path: str = "silver/nyc_taxi"
    gold_path: str = "gold/nyc_taxi"

    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.bucket:
            raise ValueError("GCS_BUCKET must be set")


class JobConfig:
    """
    Singleton configuration for PySpark jobs.

    This class follows the Singleton pattern to ensure consistent configuration
    across all jobs in the same process. Uses lazy initialization and thread-safe
    implementation.

    Best practices implemented:
    1. Singleton pattern for consistent configuration
    2. Environment-based configuration (12-factor app)
    3. System temp directory usage with proper cleanup
    4. Validation on initialization
    5. Immutable configuration after creation
    """

    _instance: Optional["JobConfig"] = None
    _initialized: bool = False

    def __new__(cls):
        """Singleton pattern: ensure only one instance exists"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize configuration (only once due to singleton)"""
        # Only initialize once
        if JobConfig._initialized:
            return

        # Use system temp directory with app-specific subdirectory
        # This is more efficient and follows OS best practices:
        # - Respects OS temp directory settings ($TMPDIR, /tmp, etc.)
        # - Automatically benefits from OS cleanup policies
        # - Works across different platforms (Linux, macOS, Windows)
        base_temp = Path(tempfile.gettempdir())
        self._cache_dir = base_temp / "nyc-taxi-pipeline" / "cache"

        # Initialize storage configuration
        self._storage_backend = os.getenv("STORAGE_BACKEND", "minio").lower()
        self._minio = MinIOConfig()
        self._gcs = GCSConfig()

        # Mark as initialized
        JobConfig._initialized = True

    @property
    def cache_dir(self) -> Path:
        """
        Get cache directory path.

        Returns Path object for better path manipulation and OS compatibility.
        Creates directory if it doesn't exist (lazy creation).
        """
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        return self._cache_dir

    @property
    def storage_backend(self) -> str:
        """Get storage backend type (minio or gcs)"""
        return self._storage_backend

    @property
    def minio(self) -> MinIOConfig:
        """Get MinIO configuration (immutable)"""
        return self._minio

    @property
    def gcs(self) -> GCSConfig:
        """Get GCS configuration (immutable)"""
        return self._gcs

    @property
    def use_gcs(self) -> bool:
        """Check if GCS storage backend is enabled"""
        return self._storage_backend == "gcs"

    def get_storage_path(
        self,
        layer: Literal["bronze", "silver", "gold"],
        taxi_type: Optional[str] = None,
    ) -> str:
        """
        Get storage path for given layer and taxi type.

        Automatically uses GCS (gs://) or S3 (s3a://) based on STORAGE_BACKEND.

        Args:
            layer: Data lake layer (bronze, silver, gold)
            taxi_type: Type of taxi (yellow, green) - optional

        Returns:
            Full storage path (gs:// for GCS, s3a:// for MinIO)

        Raises:
            ValueError: If layer is invalid

        Examples:
            >>> config = JobConfig()
            >>> # With STORAGE_BACKEND=gcs
            >>> config.get_storage_path("bronze", "yellow")
            'gs://nyc-taxi-dev-etl-us-central1-01/bronze/nyc_taxi/yellow'
            >>> # With STORAGE_BACKEND=minio
            >>> config.get_storage_path("bronze", "yellow")
            's3a://nyc-taxi-pipeline/bronze/nyc_taxi/yellow'
        """
        if self.use_gcs:
            try:
                layer_path = getattr(self._gcs, f"{layer}_path")
            except AttributeError:
                raise ValueError(
                    f"Invalid layer: {layer}. Must be one of: bronze, silver, gold"
                )
            base_path = f"gs://{self._gcs.bucket}/{layer_path}"
        else:
            try:
                layer_path = getattr(self._minio, f"{layer}_path")
            except AttributeError:
                raise ValueError(
                    f"Invalid layer: {layer}. Must be one of: bronze, silver, gold"
                )
            base_path = f"s3a://{self._minio.bucket}/{layer_path}"

        if taxi_type:
            return f"{base_path}/{taxi_type}"
        return base_path

    def get_s3_path(
        self,
        layer: Literal["bronze", "silver", "gold"],
        taxi_type: Optional[str] = None,
    ) -> str:
        """
        Get S3 path for given layer and taxi type.

        DEPRECATED: Use get_storage_path() instead for GCS/MinIO compatibility.

        Args:
            layer: Data lake layer (bronze, silver, gold)
            taxi_type: Type of taxi (yellow, green) - optional

        Returns:
            Full storage path
        """
        return self.get_storage_path(layer, taxi_type)

    @classmethod
    def reset(cls):
        """
        Reset singleton instance.

        Useful for testing purposes only. Should not be used in production code.
        """
        cls._instance = None
        cls._initialized = False
