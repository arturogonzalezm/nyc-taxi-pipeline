"""
Unit tests for BaseSparkJob and JobExecutionError.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import logging

from etl.jobs.base_job import BaseSparkJob, JobExecutionError
from etl.jobs.utils.config import JobConfig


class TestJobExecutionError:
    """Tests for JobExecutionError exception class."""

    def test_error_message(self):
        """Test JobExecutionError stores message correctly."""
        error = JobExecutionError("Job failed")
        assert str(error) == "Job failed"

    def test_error_inheritance(self):
        """Test JobExecutionError inherits from Exception."""
        error = JobExecutionError("Test")
        assert isinstance(error, Exception)

    def test_error_with_cause(self):
        """Test JobExecutionError can be raised with cause."""
        original = ValueError("Original error")
        try:
            raise JobExecutionError("Wrapper") from original
        except JobExecutionError as e:
            assert e.__cause__ is original


class ConcreteTestJob(BaseSparkJob):
    """Concrete implementation for testing BaseSparkJob."""

    def __init__(self, job_name: str, config=None):
        super().__init__(job_name, config)
        self.validate_called = False
        self.extract_called = False
        self.transform_called = False
        self.load_called = False
        self.cleanup_called = False

    def validate_inputs(self):
        self.validate_called = True

    def extract(self):
        self.extract_called = True
        return Mock()

    def transform(self, df):
        self.transform_called = True
        return df

    def load(self, df):
        self.load_called = True


class TestBaseSparkJobInit:
    """Tests for BaseSparkJob initialization."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def teardown_method(self):
        """Reset JobConfig singleton after each test."""
        JobConfig.reset()

    def test_init_with_valid_name(self):
        """Test initialization with valid job name."""
        job = ConcreteTestJob("test_job")
        assert job.job_name == "test_job"

    def test_init_strips_whitespace(self):
        """Test initialization strips whitespace from job name."""
        job = ConcreteTestJob("  test_job  ")
        assert job.job_name == "test_job"

    def test_init_empty_name_raises_error(self):
        """Test initialization with empty name raises ValueError."""
        with pytest.raises(ValueError, match="job_name cannot be empty"):
            ConcreteTestJob("")

    def test_init_whitespace_only_name_raises_error(self):
        """Test initialization with whitespace-only name raises ValueError."""
        with pytest.raises(ValueError, match="job_name cannot be empty"):
            ConcreteTestJob("   ")

    def test_init_none_name_raises_error(self):
        """Test initialization with None name raises error."""
        with pytest.raises((ValueError, TypeError)):
            ConcreteTestJob(None)

    def test_init_creates_logger(self):
        """Test initialization creates logger."""
        job = ConcreteTestJob("test_job")
        assert job.logger is not None
        assert isinstance(job.logger, logging.Logger)

    def test_init_logger_name_matches_job_name(self):
        """Test logger name matches job name."""
        job = ConcreteTestJob("my_unique_job")
        assert job.logger.name == "my_unique_job"

    def test_init_with_custom_config(self):
        """Test initialization with custom config."""
        config = JobConfig()
        job = ConcreteTestJob("test_job", config=config)
        assert job.config is config

    def test_init_uses_default_config(self):
        """Test initialization uses default config when not provided."""
        job = ConcreteTestJob("test_job")
        assert job.config is not None
        assert isinstance(job.config, JobConfig)

    def test_init_spark_is_none(self):
        """Test spark session is None after init."""
        job = ConcreteTestJob("test_job")
        assert job.spark is None

    def test_init_metrics_empty(self):
        """Test metrics dict is empty after init."""
        job = ConcreteTestJob("test_job")
        assert job._metrics == {}


class TestBaseSparkJobSetupLogger:
    """Tests for BaseSparkJob._setup_logger method."""

    def setup_method(self):
        JobConfig.reset()

    def teardown_method(self):
        JobConfig.reset()

    def test_logger_level_is_info(self):
        """Test logger level is set to INFO."""
        job = ConcreteTestJob("test_logger")
        assert job.logger.level == logging.INFO

    def test_logger_has_handler(self):
        """Test logger has at least one handler."""
        job = ConcreteTestJob("test_handler")
        assert len(job.logger.handlers) >= 1

    def test_logger_handler_is_stream_handler(self):
        """Test logger handler is StreamHandler."""
        job = ConcreteTestJob("test_stream")
        # Find the StreamHandler we added
        stream_handlers = [h for h in job.logger.handlers if isinstance(h, logging.StreamHandler)]
        assert len(stream_handlers) >= 1


class TestBaseSparkJobGetMetrics:
    """Tests for BaseSparkJob.get_metrics method."""

    def setup_method(self):
        JobConfig.reset()

    def teardown_method(self):
        JobConfig.reset()

    def test_get_metrics_returns_dict(self):
        """Test get_metrics returns a dictionary."""
        job = ConcreteTestJob("test_metrics")
        metrics = job.get_metrics()
        assert isinstance(metrics, dict)

    def test_get_metrics_returns_copy(self):
        """Test get_metrics returns a copy of metrics."""
        job = ConcreteTestJob("test_metrics_copy")
        job._metrics["test_key"] = "test_value"
        metrics = job.get_metrics()
        metrics["new_key"] = "new_value"
        assert "new_key" not in job._metrics


class TestBaseSparkJobCleanup:
    """Tests for BaseSparkJob.cleanup method."""

    def setup_method(self):
        JobConfig.reset()

    def teardown_method(self):
        JobConfig.reset()

    def test_cleanup_does_not_raise(self):
        """Test cleanup method does not raise by default."""
        job = ConcreteTestJob("test_cleanup")
        # Should not raise
        job.cleanup()


class TestBaseSparkJobValidateInputs:
    """Tests for BaseSparkJob.validate_inputs method."""

    def setup_method(self):
        JobConfig.reset()

    def teardown_method(self):
        JobConfig.reset()

    def test_validate_inputs_called(self):
        """Test validate_inputs can be called."""
        job = ConcreteTestJob("test_validate")
        job.validate_inputs()
        assert job.validate_called is True


class TestBaseSparkJobAbstractMethods:
    """Tests for abstract method requirements."""

    def setup_method(self):
        JobConfig.reset()

    def teardown_method(self):
        JobConfig.reset()

    def test_cannot_instantiate_base_class(self):
        """Test BaseSparkJob cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseSparkJob("test")

    def test_subclass_must_implement_extract(self):
        """Test subclass must implement extract method."""
        class IncompleteJob(BaseSparkJob):
            def validate_inputs(self):
                pass
            def transform(self, df):
                return df
            def load(self, df):
                pass

        with pytest.raises(TypeError):
            IncompleteJob("test")

    def test_subclass_must_implement_transform(self):
        """Test subclass must implement transform method."""
        class IncompleteJob(BaseSparkJob):
            def validate_inputs(self):
                pass
            def extract(self):
                return Mock()
            def load(self, df):
                pass

        with pytest.raises(TypeError):
            IncompleteJob("test")

    def test_subclass_must_implement_load(self):
        """Test subclass must implement load method."""
        class IncompleteJob(BaseSparkJob):
            def validate_inputs(self):
                pass
            def extract(self):
                return Mock()
            def transform(self, df):
                return df

        with pytest.raises(TypeError):
            IncompleteJob("test")
