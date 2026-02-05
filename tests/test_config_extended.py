"""
Extended tests for configuration module to improve coverage.
"""

import os
import pytest
from unittest.mock import patch

from etl.jobs.utils.config import GCSConfig, JobConfig


class TestGCSConfig:
    """Tests for GCSConfig dataclass."""

    def test_default_values_from_tfvars(self):
        """Test GCSConfig reads from terraform.tfvars when env vars not set."""
        with patch.dict(os.environ, {}, clear=True):
            # Should fall back to terraform.tfvars
            config = GCSConfig()
            assert config.bucket == "nyc-taxi-pipeline-dev-gcs-us-central1-001"
            assert config.project_id == "nyc-taxi-pipeline-dev-us-central1-001"

    def test_custom_values_from_env(self):
        """Test GCSConfig reads from environment variables."""
        with patch.dict(
            os.environ,
            {
                "GCS_BUCKET": "custom-gcs-bucket",
                "GCP_PROJECT_ID": "custom-project",
            },
        ):
            config = GCSConfig()
            assert config.bucket == "custom-gcs-bucket"
            assert config.project_id == "custom-project"

    def test_env_vars_override_tfvars(self):
        """Test that env vars take priority over terraform.tfvars."""
        with patch.dict(
            os.environ,
            {
                "GCS_BUCKET": "override-bucket",
                "GCP_PROJECT_ID": "override-project",
            },
        ):
            config = GCSConfig()
            assert config.bucket == "override-bucket"
            assert config.project_id == "override-project"


class TestJobConfigExtended:
    """Extended tests for JobConfig singleton."""

    def setup_method(self):
        """Reset singleton before each test."""
        JobConfig.reset()

    def teardown_method(self):
        """Reset singleton after each test."""
        JobConfig.reset()

    def test_singleton_returns_same_instance(self):
        """Test that JobConfig returns the same instance."""
        with patch.dict(os.environ, {"GCS_BUCKET": "test-bucket"}):
            config1 = JobConfig()
            config2 = JobConfig()
            assert config1 is config2

    def test_cache_dir_creates_directory(self, tmp_path):
        """Test that cache_dir property creates directory if not exists."""
        with patch.dict(os.environ, {"GCS_BUCKET": "test-bucket"}):
            config = JobConfig()
            cache_dir = config.cache_dir
            assert cache_dir.exists()
            assert cache_dir.is_dir()

    def test_gcs_property_returns_config(self):
        """Test that gcs property returns GCSConfig instance."""
        with patch.dict(
            os.environ,
            {
                "GCS_BUCKET": "test-bucket",
                "GCP_PROJECT_ID": "test-project",
            },
        ):
            config = JobConfig()
            assert isinstance(config.gcs, GCSConfig)

    def test_get_storage_path_gcs_bronze(self):
        """Test get_storage_path for GCS bronze layer."""
        with patch.dict(
            os.environ,
            {
                "GCS_BUCKET": "my-gcs-bucket",
            },
        ):
            config = JobConfig()
            path = config.get_storage_path("bronze", "yellow")
            assert path == "gs://my-gcs-bucket/bronze/nyc_taxi/yellow"

    def test_get_storage_path_gcs_silver(self):
        """Test get_storage_path for GCS silver layer."""
        with patch.dict(
            os.environ,
            {
                "GCS_BUCKET": "my-gcs-bucket",
            },
        ):
            config = JobConfig()
            path = config.get_storage_path("silver", "green")
            assert path == "gs://my-gcs-bucket/silver/nyc_taxi/green"

    def test_get_storage_path_gcs_gold(self):
        """Test get_storage_path for GCS gold layer."""
        with patch.dict(
            os.environ,
            {
                "GCS_BUCKET": "my-gcs-bucket",
            },
        ):
            config = JobConfig()
            path = config.get_storage_path("gold")
            assert path == "gs://my-gcs-bucket/gold/nyc_taxi"

    def test_get_storage_path_without_taxi_type(self):
        """Test get_storage_path without taxi_type returns base path."""
        with patch.dict(
            os.environ,
            {
                "GCS_BUCKET": "my-gcs-bucket",
            },
        ):
            config = JobConfig()
            path = config.get_storage_path("bronze")
            assert path == "gs://my-gcs-bucket/bronze/nyc_taxi"

    def test_get_storage_path_invalid_layer(self):
        """Test get_storage_path raises ValueError for invalid layer."""
        with patch.dict(
            os.environ,
            {
                "GCS_BUCKET": "my-bucket",
            },
        ):
            config = JobConfig()
            with pytest.raises(ValueError, match="Invalid layer"):
                config.get_storage_path("invalid")

    def test_reset_clears_singleton(self):
        """Test that reset() clears the singleton instance."""
        with patch.dict(os.environ, {"GCS_BUCKET": "test-bucket"}):
            JobConfig()
            JobConfig.reset()
            JobConfig()
            # After reset, should be a new instance
            assert JobConfig._initialized is True
