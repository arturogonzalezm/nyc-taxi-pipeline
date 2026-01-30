"""
Tests for SparkSessionManager class.

Tests cover:
- SparkSessionError exception
- S3 configuration validation
- Environment variable handling
"""

import os
import pytest
from unittest.mock import patch, MagicMock

from etl.jobs.utils.spark_manager import SparkSessionManager, SparkSessionError


class TestSparkSessionError:
    """Tests for SparkSessionError exception class."""

    def test_spark_session_error_message(self):
        """Test SparkSessionError stores message correctly."""
        error = SparkSessionError("Session creation failed")
        assert str(error) == "Session creation failed"

    def test_spark_session_error_with_cause(self):
        """Test SparkSessionError with cause exception."""
        cause = RuntimeError("Underlying error")
        error = SparkSessionError("Session creation failed")
        error.__cause__ = cause
        assert error.__cause__ is cause

    def test_spark_session_error_inheritance(self):
        """Test SparkSessionError inherits from Exception."""
        error = SparkSessionError("test")
        assert isinstance(error, Exception)


class TestSparkSessionManagerValidateS3Config:
    """Tests for SparkSessionManager._validate_s3_config method."""

    def test_validate_s3_config_returns_dict(self):
        """Test that _validate_s3_config returns a dictionary."""
        config = SparkSessionManager._validate_s3_config()
        assert isinstance(config, dict)

    def test_validate_s3_config_contains_endpoint(self):
        """Test that config contains endpoint."""
        config = SparkSessionManager._validate_s3_config()
        assert "endpoint" in config

    def test_validate_s3_config_contains_access_key(self):
        """Test that config contains access_key."""
        config = SparkSessionManager._validate_s3_config()
        assert "access_key" in config

    def test_validate_s3_config_contains_secret_key(self):
        """Test that config contains secret_key."""
        config = SparkSessionManager._validate_s3_config()
        assert "secret_key" in config

    def test_validate_s3_config_uses_env_vars(self):
        """Test that config uses environment variables."""
        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "custom-endpoint:9000",
                "MINIO_ACCESS_KEY": "custom-access",
                "MINIO_SECRET_KEY": "custom-secret",
            },
        ):
            config = SparkSessionManager._validate_s3_config()
            assert config["endpoint"] == "custom-endpoint:9000"
            assert config["access_key"] == "custom-access"
            assert config["secret_key"] == "custom-secret"

    def test_validate_s3_config_default_endpoint(self):
        """Test default endpoint when env var not set."""
        with patch.dict(os.environ, {}, clear=False):
            # Remove MINIO_ENDPOINT if it exists
            env_copy = os.environ.copy()
            env_copy.pop("MINIO_ENDPOINT", None)
            with patch.dict(os.environ, env_copy, clear=True):
                config = SparkSessionManager._validate_s3_config()
                assert "localhost:9000" in config["endpoint"]

    def test_validate_s3_config_default_access_key(self):
        """Test default access key when env var not set."""
        with patch.dict(os.environ, {}, clear=False):
            env_copy = os.environ.copy()
            env_copy.pop("MINIO_ACCESS_KEY", None)
            with patch.dict(os.environ, env_copy, clear=True):
                config = SparkSessionManager._validate_s3_config()
                assert config["access_key"] == "minioadmin"

    def test_validate_s3_config_default_secret_key(self):
        """Test default secret key when env var not set."""
        with patch.dict(os.environ, {}, clear=False):
            env_copy = os.environ.copy()
            env_copy.pop("MINIO_SECRET_KEY", None)
            with patch.dict(os.environ, env_copy, clear=True):
                config = SparkSessionManager._validate_s3_config()
                assert config["secret_key"] == "minioadmin"

    def test_validate_s3_config_empty_endpoint_raises_error(self):
        """Test that empty endpoint raises SparkSessionError."""
        with patch.dict(os.environ, {"MINIO_ENDPOINT": ""}):
            with pytest.raises(SparkSessionError) as exc_info:
                SparkSessionManager._validate_s3_config()
            assert "cannot be empty" in str(exc_info.value)

    def test_validate_s3_config_strips_http_prefix(self):
        """Test that endpoint with http:// prefix is stripped."""
        with patch.dict(os.environ, {"MINIO_ENDPOINT": "http://localhost:9000"}):
            # Should strip the protocol prefix
            config = SparkSessionManager._validate_s3_config()
            assert config["endpoint"] == "localhost:9000"

    def test_validate_s3_config_strips_https_prefix(self):
        """Test that endpoint with https:// prefix is stripped."""
        with patch.dict(os.environ, {"MINIO_ENDPOINT": "https://localhost:9000"}):
            # Should strip the protocol prefix
            config = SparkSessionManager._validate_s3_config()
            assert config["endpoint"] == "localhost:9000"


class TestSparkSessionManagerClassAttributes:
    """Tests for SparkSessionManager class attributes."""

    def test_instance_initially_none(self):
        """Test that _instance starts as None or SparkSession."""
        # _instance could be None or an existing session
        assert SparkSessionManager._instance is None or hasattr(
            SparkSessionManager._instance, "sparkContext"
        )

    def test_session_config_is_dict(self):
        """Test that _session_config is a dictionary."""
        assert isinstance(SparkSessionManager._session_config, dict)


class TestSparkSessionManagerStopSession:
    """Tests for SparkSessionManager.stop_session method."""

    def test_stop_session_when_no_session(self):
        """Test stop_session when no session exists."""
        # Save current instance
        original_instance = SparkSessionManager._instance
        SparkSessionManager._instance = None

        # Should not raise
        SparkSessionManager.stop_session()

        # Restore
        SparkSessionManager._instance = original_instance

    def test_stop_session_clears_instance(self):
        """Test that stop_session clears the instance."""
        # Create a mock session
        mock_session = MagicMock()
        SparkSessionManager._instance = mock_session

        SparkSessionManager.stop_session()

        assert SparkSessionManager._instance is None
        mock_session.stop.assert_called_once()
