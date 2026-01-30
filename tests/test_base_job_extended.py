"""
Extended tests for BaseSparkJob to improve coverage.
"""

import pytest

from etl.jobs.base_job import BaseSparkJob, JobExecutionError
from etl.jobs.utils.spark_manager import SparkSessionManager


class _DummySpark:
    """Mock Spark session for testing."""

    pass


class _SuccessJob(BaseSparkJob):
    """Job that succeeds."""

    def validate_inputs(self):
        pass

    def extract(self):
        return {"data": "test"}

    def transform(self, df):
        return df

    def load(self, df):
        pass


class _FailingValidationJob(BaseSparkJob):
    """Job that fails during validation."""

    def validate_inputs(self):
        raise ValueError("Validation failed")

    def extract(self):
        return {"data": "test"}

    def transform(self, df):
        return df

    def load(self, df):
        pass


class _FailingTransformJob(BaseSparkJob):
    """Job that returns None from transform."""

    def validate_inputs(self):
        pass

    def extract(self):
        return {"data": "test"}

    def transform(self, df):
        return None

    def load(self, df):
        pass


class _FailingLoadJob(BaseSparkJob):
    """Job that fails during load."""

    def validate_inputs(self):
        pass

    def extract(self):
        return {"data": "test"}

    def transform(self, df):
        return df

    def load(self, df):
        raise RuntimeError("Load failed")


class _JobWithCleanup(BaseSparkJob):
    """Job that tracks cleanup calls."""

    cleanup_called = False

    def validate_inputs(self):
        pass

    def extract(self):
        return {"data": "test"}

    def transform(self, df):
        return df

    def load(self, df):
        pass

    def cleanup(self):
        _JobWithCleanup.cleanup_called = True
        super().cleanup()


class TestBaseSparkJobExtended:
    """Extended tests for BaseSparkJob."""

    def test_job_name_property(self, monkeypatch):
        """Test job_name property returns correct name."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )
        job = _SuccessJob("test_job_name")
        assert job.job_name == "test_job_name"

    def test_get_metrics_returns_dict(self, monkeypatch):
        """Test get_metrics returns metrics dictionary."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )
        job = _SuccessJob("metrics_test")
        job.run()
        metrics = job.get_metrics()
        assert isinstance(metrics, dict)
        assert "job_name" in metrics
        assert metrics["job_name"] == "metrics_test"

    def test_metrics_contain_timing_info(self, monkeypatch):
        """Test metrics contain timing information after run."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )
        job = _SuccessJob("timing_test")
        job.run()
        metrics = job.get_metrics()
        assert "start_time" in metrics
        assert "total_duration_seconds" in metrics
        assert "status" in metrics
        assert metrics["status"] == "SUCCESS"

    def test_validation_failure_raises_error(self, monkeypatch):
        """Test that validation failure raises JobExecutionError."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )
        job = _FailingValidationJob("validation_fail")
        with pytest.raises(JobExecutionError):
            job.run()

    def test_transform_none_raises_error(self, monkeypatch):
        """Test that transform returning None raises JobExecutionError."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )
        job = _FailingTransformJob("transform_fail")
        with pytest.raises(JobExecutionError, match="Transform step returned None"):
            job.run()

    def test_load_failure_raises_error(self, monkeypatch):
        """Test that load failure raises JobExecutionError."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )
        job = _FailingLoadJob("load_fail")
        with pytest.raises(JobExecutionError):
            job.run()

    def test_cleanup_called_on_success(self, monkeypatch):
        """Test that cleanup is called after successful run."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )
        _JobWithCleanup.cleanup_called = False
        job = _JobWithCleanup("cleanup_success")
        job.run()
        assert _JobWithCleanup.cleanup_called is True

    def test_cleanup_called_on_failure(self, monkeypatch):
        """Test that cleanup is called even after failure."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )

        class _FailingJobWithCleanup(_JobWithCleanup):
            def extract(self):
                return None

        _JobWithCleanup.cleanup_called = False
        job = _FailingJobWithCleanup("cleanup_failure")
        with pytest.raises(JobExecutionError):
            job.run()
        assert _JobWithCleanup.cleanup_called is True

    def test_metrics_contain_error_info_on_failure(self, monkeypatch):
        """Test metrics contain error information on failure."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )
        job = _FailingLoadJob("error_metrics")
        with pytest.raises(JobExecutionError):
            job.run()
        metrics = job.get_metrics()
        assert metrics["status"] == "FAILED"
        assert "error_message" in metrics
        assert "error_type" in metrics

    def test_job_execution_error_preserves_original(self, monkeypatch):
        """Test JobExecutionError preserves original exception."""
        monkeypatch.setattr(
            SparkSessionManager,
            "get_session",
            lambda app_name, enable_s3: _DummySpark(),
        )
        job = _FailingLoadJob("preserve_error")
        with pytest.raises(JobExecutionError) as exc_info:
            job.run()
        assert exc_info.value.__cause__ is not None


class TestJobExecutionError:
    """Tests for JobExecutionError exception class."""

    def test_job_execution_error_message(self):
        """Test JobExecutionError stores message correctly."""
        error = JobExecutionError("Test error message")
        assert str(error) == "Test error message"

    def test_job_execution_error_with_cause(self):
        """Test JobExecutionError can be raised with cause."""
        original = ValueError("Original error")
        try:
            raise JobExecutionError("Wrapped error") from original
        except JobExecutionError as e:
            assert e.__cause__ is original
