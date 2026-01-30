"""
Comprehensive tests for BaseSparkJob class to improve coverage.
"""

import pytest

from etl.jobs.base_job import BaseSparkJob, JobExecutionError
from etl.jobs.utils.config import JobConfig
from etl.jobs.utils.spark_manager import SparkSessionManager


class _DummySpark:
    """Dummy Spark session for testing."""

    pass


class _SuccessfulJob(BaseSparkJob):
    """A job that completes successfully."""

    def __init__(self, job_name="test_job", config=None):
        super().__init__(job_name, config)
        self.validated = False
        self.extracted = False
        self.transformed = False
        self.loaded = False
        self.cleaned_up = False

    def validate_inputs(self):
        self.validated = True

    def extract(self):
        self.extracted = True
        return {"data": [1, 2, 3]}

    def transform(self, df):
        self.transformed = True
        return {"transformed": df}

    def load(self, df):
        self.loaded = True

    def cleanup(self):
        self.cleaned_up = True
        super().cleanup()


class _FailingValidationJob(BaseSparkJob):
    """A job that fails during validation."""

    def validate_inputs(self):
        raise ValueError("Validation failed")

    def extract(self):
        return {}

    def transform(self, df):
        return df

    def load(self, df):
        pass


class _FailingExtractJob(BaseSparkJob):
    """A job that returns None from extract."""

    def validate_inputs(self):
        pass

    def extract(self):
        return None

    def transform(self, df):
        return df

    def load(self, df):
        pass


class _FailingTransformJob(BaseSparkJob):
    """A job that returns None from transform."""

    def validate_inputs(self):
        pass

    def extract(self):
        return {"data": [1, 2, 3]}

    def transform(self, df):
        return None

    def load(self, df):
        pass


class _FailingLoadJob(BaseSparkJob):
    """A job that raises exception during load."""

    def validate_inputs(self):
        pass

    def extract(self):
        return {"data": [1, 2, 3]}

    def transform(self, df):
        return {"transformed": True}

    def load(self, df):
        raise RuntimeError("Load failed")


class _ExceptionInTransformJob(BaseSparkJob):
    """A job that raises exception during transform."""

    def validate_inputs(self):
        pass

    def extract(self):
        return {"data": [1, 2, 3]}

    def transform(self, df):
        raise ValueError("Transform error")

    def load(self, df):
        pass


class TestBaseSparkJobRun:
    """Tests for BaseSparkJob.run method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_run_successful_job(self, monkeypatch):
        """Test running a successful job."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )

        job = _SuccessfulJob("test_success")
        result = job.run()

        assert result is True
        assert job.validated is True
        assert job.extracted is True
        assert job.transformed is True
        assert job.loaded is True
        assert job.cleaned_up is True

    def test_run_sets_metrics_on_success(self, monkeypatch):
        """Test that metrics are set correctly on success."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )

        job = _SuccessfulJob("test_metrics")
        job.run()

        metrics = job.get_metrics()
        assert metrics["status"] == "SUCCESS"
        assert "total_duration_seconds" in metrics
        assert "start_time" in metrics
        assert metrics["job_name"] == "test_metrics"

    def test_run_failing_validation_raises_error(self, monkeypatch):
        """Test that validation failure raises JobExecutionError."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )

        job = _FailingValidationJob("test_validation_fail")
        with pytest.raises(JobExecutionError):
            job.run()

    def test_run_extract_none_raises_error(self, monkeypatch):
        """Test that extract returning None raises JobExecutionError."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )

        job = _FailingExtractJob("test_extract_none")
        with pytest.raises(JobExecutionError) as exc_info:
            job.run()
        assert "Extract step returned None" in str(exc_info.value)

    def test_run_transform_none_raises_error(self, monkeypatch):
        """Test that transform returning None raises JobExecutionError."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )

        job = _FailingTransformJob("test_transform_none")
        with pytest.raises(JobExecutionError) as exc_info:
            job.run()
        assert "Transform step returned None" in str(exc_info.value)

    def test_run_load_failure_raises_error(self, monkeypatch):
        """Test that load failure raises JobExecutionError."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )

        job = _FailingLoadJob("test_load_fail")
        with pytest.raises(JobExecutionError) as exc_info:
            job.run()
        assert "Load failed" in str(exc_info.value)

    def test_run_transform_exception_raises_error(self, monkeypatch):
        """Test that transform exception raises JobExecutionError."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )

        job = _ExceptionInTransformJob("test_transform_exception")
        with pytest.raises(JobExecutionError) as exc_info:
            job.run()
        assert "Transform error" in str(exc_info.value)

    def test_run_sets_error_metrics_on_failure(self, monkeypatch):
        """Test that error metrics are set on failure."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )

        job = _FailingLoadJob("test_error_metrics")
        try:
            job.run()
        except JobExecutionError:
            pass

        metrics = job.get_metrics()
        assert metrics["status"] == "FAILED"
        assert "error_message" in metrics
        assert "error_type" in metrics

    def test_cleanup_called_on_failure(self, monkeypatch):
        """Test that cleanup is called even on failure."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )

        job = _SuccessfulJob("test_cleanup_on_fail")
        # Override load to fail
        job.load = lambda df: (_ for _ in ()).throw(RuntimeError("Load failed"))

        try:
            job.run()
        except JobExecutionError:
            pass

        assert job.cleaned_up is True


class TestBaseSparkJobProperties:
    """Tests for BaseSparkJob properties."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_job_name_property(self):
        """Test job_name property returns correct name."""
        job = _SuccessfulJob("my_test_job")
        assert job.job_name == "my_test_job"

    def test_config_property(self):
        """Test config property returns JobConfig."""
        job = _SuccessfulJob("test_config")
        assert isinstance(job.config, JobConfig)

    def test_logger_property(self):
        """Test logger property returns a logger."""
        job = _SuccessfulJob("test_logger")
        assert job.logger is not None
        assert hasattr(job.logger, "info")
        assert hasattr(job.logger, "error")

    def test_get_metrics_returns_dict(self):
        """Test get_metrics returns a dictionary."""
        job = _SuccessfulJob("test_get_metrics")
        metrics = job.get_metrics()
        assert isinstance(metrics, dict)

    def test_spark_property_before_run(self):
        """Test spark property is None before run."""
        job = _SuccessfulJob("test_spark_before")
        assert job.spark is None


class TestJobExecutionError:
    """Tests for JobExecutionError exception."""

    def test_error_message(self):
        """Test error message is stored correctly."""
        error = JobExecutionError("Test error message")
        assert str(error) == "Test error message"

    def test_error_with_cause(self):
        """Test error with cause exception."""
        cause = ValueError("Original error")
        error = JobExecutionError("Wrapped error")
        error.__cause__ = cause
        assert error.__cause__ is cause

    def test_error_inheritance(self):
        """Test JobExecutionError inherits from Exception."""
        error = JobExecutionError("test")
        assert isinstance(error, Exception)

    def test_raise_from_syntax(self):
        """Test raise from syntax works correctly."""
        original = ValueError("Original")
        try:
            raise JobExecutionError("Wrapped") from original
        except JobExecutionError as e:
            assert e.__cause__ is original
