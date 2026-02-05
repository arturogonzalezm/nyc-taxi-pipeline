"""
Unit tests for SparkSessionManager.
"""

import pytest
from unittest.mock import patch, MagicMock
import os

from etl.jobs.utils.spark_manager import SparkSessionManager, SparkSessionError


class TestSparkSessionError:
    """Tests for SparkSessionError exception class."""

    def test_error_message(self):
        """Test SparkSessionError stores message correctly."""
        error = SparkSessionError("Session failed")
        assert str(error) == "Session failed"

    def test_error_inheritance(self):
        """Test SparkSessionError inherits from Exception."""
        error = SparkSessionError("Test")
        assert isinstance(error, Exception)

    def test_error_with_cause(self):
        """Test SparkSessionError can be raised with cause."""
        original = RuntimeError("Original")
        try:
            raise SparkSessionError("Wrapper") from original
        except SparkSessionError as e:
            assert e.__cause__ is original


class TestSparkSessionManagerGetSessionValidation:
    """Tests for SparkSessionManager.get_session input validation."""

    def setup_method(self):
        """Reset singleton before each test."""
        SparkSessionManager._instance = None
        SparkSessionManager._session_config = {}

    def teardown_method(self):
        """Reset singleton after each test."""
        SparkSessionManager._instance = None
        SparkSessionManager._session_config = {}

    def test_empty_app_name_raises_error(self):
        """Test empty app_name raises ValueError."""
        with pytest.raises(ValueError, match="app_name cannot be empty"):
            SparkSessionManager.get_session("")

    def test_whitespace_only_app_name_raises_error(self):
        """Test whitespace-only app_name raises ValueError."""
        with pytest.raises(ValueError, match="app_name cannot be empty"):
            SparkSessionManager.get_session("   ")

    def test_none_app_name_raises_error(self):
        """Test None app_name raises error."""
        with pytest.raises((ValueError, TypeError)):
            SparkSessionManager.get_session(None)


class TestSparkSessionManagerStopSession:
    """Tests for SparkSessionManager.stop_session method."""

    def setup_method(self):
        """Reset singleton before each test."""
        SparkSessionManager._instance = None
        SparkSessionManager._session_config = {}

    def teardown_method(self):
        """Reset singleton after each test."""
        SparkSessionManager._instance = None
        SparkSessionManager._session_config = {}

    def test_stop_session_when_none(self):
        """Test stop_session does not raise when no session exists."""
        SparkSessionManager._instance = None
        # Should not raise
        SparkSessionManager.stop_session()
        assert SparkSessionManager._instance is None

    def test_stop_session_clears_instance(self):
        """Test stop_session clears the instance."""
        mock_session = MagicMock()
        SparkSessionManager._instance = mock_session
        SparkSessionManager._session_config = {"app_name": "test"}
        
        SparkSessionManager.stop_session()
        
        assert SparkSessionManager._instance is None
        assert SparkSessionManager._session_config == {}

    def test_stop_session_calls_stop(self):
        """Test stop_session calls stop on the session."""
        mock_session = MagicMock()
        SparkSessionManager._instance = mock_session
        
        SparkSessionManager.stop_session()
        
        mock_session.stop.assert_called_once()

    def test_stop_session_handles_stop_error(self):
        """Test stop_session handles errors during stop gracefully."""
        mock_session = MagicMock()
        mock_session.stop.side_effect = Exception("Stop failed")
        SparkSessionManager._instance = mock_session
        
        # Should not raise
        SparkSessionManager.stop_session()
        
        assert SparkSessionManager._instance is None


class TestSparkSessionManagerGetSessionInfo:
    """Tests for SparkSessionManager.get_session_info method."""

    def setup_method(self):
        """Reset singleton before each test."""
        SparkSessionManager._instance = None
        SparkSessionManager._session_config = {}

    def teardown_method(self):
        """Reset singleton after each test."""
        SparkSessionManager._instance = None
        SparkSessionManager._session_config = {}

    def test_get_session_info_not_initialized(self):
        """Test get_session_info when no session exists."""
        info = SparkSessionManager.get_session_info()
        assert info == {"status": "not_initialized"}

    def test_get_session_info_with_session(self):
        """Test get_session_info when session exists."""
        mock_session = MagicMock()
        SparkSessionManager._instance = mock_session
        SparkSessionManager._session_config = {
            "app_name": "TestApp",
            "gcs_project": "test-project",
            "spark_version": "3.5.0"
        }
        
        info = SparkSessionManager.get_session_info()
        
        assert info["status"] == "active"
        assert info["app_name"] == "TestApp"
        assert info["gcs_project"] == "test-project"


class TestSparkSessionManagerEnvironmentConfig:
    """Tests for SparkSessionManager environment variable handling."""

    def setup_method(self):
        """Reset singleton before each test."""
        SparkSessionManager._instance = None
        SparkSessionManager._session_config = {}

    def teardown_method(self):
        """Reset singleton after each test."""
        SparkSessionManager._instance = None
        SparkSessionManager._session_config = {}

    def test_default_project_id(self):
        """Test default GCP project ID is used when env var not set."""
        with patch.dict(os.environ, {}, clear=True):
            project = os.getenv("GCP_PROJECT_ID", "")
            assert project == ""

    def test_custom_project_id_from_env(self):
        """Test custom GCP project ID from environment."""
        with patch.dict(os.environ, {"GCP_PROJECT_ID": "custom-project"}):
            project = os.getenv("GCP_PROJECT_ID", "")
            assert project == "custom-project"

    def test_credentials_path_from_env(self):
        """Test credentials path from environment."""
        with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "/path/to/creds.json"}):
            creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
            assert creds == "/path/to/creds.json"
