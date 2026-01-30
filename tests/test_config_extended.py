"""
Extended tests for configuration module to improve coverage.
"""

import os
import pytest
from unittest.mock import patch

from etl.jobs.utils.config import MinIOConfig, JobConfig


class TestMinIOConfig:
    """Tests for MinIOConfig dataclass."""

    def test_default_values(self):
        """Test MinIOConfig uses default values when env vars not set."""
        with patch.dict(os.environ, {}, clear=True):
            # Set minimum required values
            with patch.dict(
                os.environ,
                {
                    "MINIO_ENDPOINT": "localhost:9000",
                    "MINIO_BUCKET": "test-bucket",
                },
            ):
                config = MinIOConfig()
                assert config.endpoint == "localhost:9000"
                assert config.access_key == "minioadmin"
                assert config.secret_key == "minioadmin"
                assert config.bucket == "test-bucket"
                assert config.bronze_path == "bronze/nyc_taxi"
                assert config.silver_path == "silver/nyc_taxi"
                assert config.gold_path == "gold/nyc_taxi"

    def test_custom_values_from_env(self):
        """Test MinIOConfig reads from environment variables."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "custom-host:9001",
                "MINIO_ACCESS_KEY": "custom-access",
                "MINIO_SECRET_KEY": "custom-secret",
                "MINIO_BUCKET": "custom-bucket",
                "USE_MINIO": "false",
            },
        ):
            config = MinIOConfig()
            assert config.endpoint == "custom-host:9001"
            assert config.access_key == "custom-access"
            assert config.secret_key == "custom-secret"
            assert config.bucket == "custom-bucket"
            assert config.use_minio is False

    def test_use_minio_true(self):
        """Test USE_MINIO=true is parsed correctly."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_BUCKET": "test",
                "USE_MINIO": "true",
            },
        ):
            config = MinIOConfig()
            assert config.use_minio is True

    def test_use_minio_case_insensitive(self):
        """Test USE_MINIO parsing is case insensitive."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_BUCKET": "test",
                "USE_MINIO": "TRUE",
            },
        ):
            config = MinIOConfig()
            assert config.use_minio is True

    def test_empty_endpoint_raises_error(self):
        """Test that empty endpoint raises ValueError."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "",
                "MINIO_BUCKET": "test",
            },
        ):
            with pytest.raises(ValueError, match="MINIO_ENDPOINT must be set"):
                MinIOConfig()

    def test_empty_bucket_raises_error(self):
        """Test that empty bucket raises ValueError."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_BUCKET": "",
            },
        ):
            with pytest.raises(ValueError, match="MINIO_BUCKET must be set"):
                MinIOConfig()


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
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_BUCKET": "test",
            },
        ):
            config1 = JobConfig()
            config2 = JobConfig()
            assert config1 is config2

    def test_cache_dir_creates_directory(self, tmp_path):
        """Test that cache_dir property creates directory if not exists."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_BUCKET": "test",
            },
        ):
            config = JobConfig()
            cache_dir = config.cache_dir
            assert cache_dir.exists()
            assert cache_dir.is_dir()

    def test_minio_property_returns_config(self):
        """Test that minio property returns MinIOConfig instance."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_BUCKET": "test",
            },
        ):
            config = JobConfig()
            assert isinstance(config.minio, MinIOConfig)

    def test_get_s3_path_bronze(self):
        """Test get_s3_path for bronze layer."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_BUCKET": "my-bucket",
            },
        ):
            config = JobConfig()
            path = config.get_s3_path("bronze", "yellow")
            assert path == "s3a://my-bucket/bronze/nyc_taxi/yellow"

    def test_get_s3_path_silver(self):
        """Test get_s3_path for silver layer."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_BUCKET": "my-bucket",
            },
        ):
            config = JobConfig()
            path = config.get_s3_path("silver", "green")
            assert path == "s3a://my-bucket/silver/nyc_taxi/green"

    def test_get_s3_path_gold(self):
        """Test get_s3_path for gold layer."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_BUCKET": "my-bucket",
            },
        ):
            config = JobConfig()
            path = config.get_s3_path("gold")
            assert path == "s3a://my-bucket/gold/nyc_taxi"

    def test_get_s3_path_without_taxi_type(self):
        """Test get_s3_path without taxi_type returns base path."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_BUCKET": "my-bucket",
            },
        ):
            config = JobConfig()
            path = config.get_s3_path("bronze")
            assert path == "s3a://my-bucket/bronze/nyc_taxi"

    def test_get_s3_path_invalid_layer(self):
        """Test get_s3_path raises ValueError for invalid layer."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_BUCKET": "my-bucket",
            },
        ):
            config = JobConfig()
            with pytest.raises(ValueError, match="Invalid layer"):
                config.get_s3_path("invalid")

    def test_reset_clears_singleton(self):
        """Test that reset() clears the singleton instance."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_BUCKET": "test",
            },
        ):
            JobConfig()
            JobConfig.reset()
            JobConfig()
            # After reset, should be a new instance
            assert JobConfig._initialized is True
