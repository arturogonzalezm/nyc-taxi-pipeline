"""
Unit tests for TaxiIngestionJob.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from etl.jobs.bronze.taxi_ingestion_job import (
    TaxiIngestionJob,
    DataValidationError,
    DownloadError,
    run_ingestion,
)
from etl.jobs.base_job import JobExecutionError
from etl.jobs.utils.config import JobConfig


class TestDataValidationError:
    """Tests for DataValidationError exception class."""

    def test_error_message(self):
        """Test DataValidationError stores message correctly."""
        error = DataValidationError("Validation failed")
        assert str(error) == "Validation failed"

    def test_error_inheritance(self):
        """Test DataValidationError inherits from JobExecutionError."""
        error = DataValidationError("Test")
        assert isinstance(error, JobExecutionError)
        assert isinstance(error, Exception)


class TestDownloadError:
    """Tests for DownloadError exception class."""

    def test_error_message(self):
        """Test DownloadError stores message correctly."""
        error = DownloadError("Download failed")
        assert str(error) == "Download failed"

    def test_error_inheritance(self):
        """Test DownloadError inherits from JobExecutionError."""
        error = DownloadError("Test")
        assert isinstance(error, JobExecutionError)
        assert isinstance(error, Exception)

    def test_error_with_cause(self):
        """Test DownloadError can be raised with cause."""
        original = IOError("Network error")
        try:
            raise DownloadError("Download failed") from original
        except DownloadError as e:
            assert e.__cause__ is original


class TestTaxiIngestionJobInit:
    """Tests for TaxiIngestionJob initialization."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def teardown_method(self):
        """Reset JobConfig singleton after each test."""
        JobConfig.reset()

    def test_init_yellow_taxi(self):
        """Test initialization with yellow taxi type."""
        job = TaxiIngestionJob("yellow", 2024, 1)
        assert job.taxi_type == "yellow"
        assert job.year == 2024
        assert job.month == 1

    def test_init_green_taxi(self):
        """Test initialization with green taxi type."""
        job = TaxiIngestionJob("green", 2024, 6)
        assert job.taxi_type == "green"
        assert job.year == 2024
        assert job.month == 6

    def test_init_file_name_format(self):
        """Test file name is generated correctly."""
        job = TaxiIngestionJob("yellow", 2024, 1)
        assert job.file_name == "yellow_tripdata_2024-01.parquet"

    def test_init_file_name_format_double_digit_month(self):
        """Test file name with double digit month."""
        job = TaxiIngestionJob("green", 2023, 12)
        assert job.file_name == "green_tripdata_2023-12.parquet"

    def test_init_job_name_format(self):
        """Test job name is generated correctly."""
        job = TaxiIngestionJob("yellow", 2024, 3)
        assert job.job_name == "TaxiIngestion_yellow_2024_03"

    def test_init_invalid_taxi_type(self):
        """Test initialization with invalid taxi type raises error."""
        with pytest.raises(ValueError, match="Invalid taxi_type"):
            TaxiIngestionJob("blue", 2024, 1)

    def test_init_invalid_year_too_low(self):
        """Test initialization with year before 2009 raises error."""
        with pytest.raises(ValueError, match="Invalid year"):
            TaxiIngestionJob("yellow", 2008, 1)

    def test_init_invalid_month_zero(self):
        """Test initialization with month 0 raises error."""
        with pytest.raises(ValueError, match="Invalid month"):
            TaxiIngestionJob("yellow", 2024, 0)

    def test_init_invalid_month_thirteen(self):
        """Test initialization with month 13 raises error."""
        with pytest.raises(ValueError, match="Invalid month"):
            TaxiIngestionJob("yellow", 2024, 13)

    def test_init_invalid_month_negative(self):
        """Test initialization with negative month raises error."""
        with pytest.raises(ValueError, match="Invalid month"):
            TaxiIngestionJob("yellow", 2024, -1)

    def test_init_with_custom_config(self):
        """Test initialization with custom config."""
        config = JobConfig()
        job = TaxiIngestionJob("yellow", 2024, 1, config=config)
        assert job.config is config


class TestTaxiIngestionJobValidateParameters:
    """Tests for TaxiIngestionJob._validate_parameters static method."""

    def test_validate_valid_parameters(self):
        """Test validation passes for valid parameters."""
        # Should not raise
        TaxiIngestionJob._validate_parameters("yellow", 2024, 1)
        TaxiIngestionJob._validate_parameters("green", 2009, 12)

    def test_validate_invalid_taxi_type(self):
        """Test validation fails for invalid taxi type."""
        with pytest.raises(ValueError, match="Invalid taxi_type"):
            TaxiIngestionJob._validate_parameters("red", 2024, 1)

    def test_validate_invalid_year_string(self):
        """Test validation fails for string year."""
        with pytest.raises(ValueError, match="Invalid year"):
            TaxiIngestionJob._validate_parameters("yellow", "2024", 1)

    def test_validate_invalid_month_string(self):
        """Test validation fails for string month."""
        with pytest.raises(ValueError, match="Invalid month"):
            TaxiIngestionJob._validate_parameters("yellow", 2024, "1")


class TestTaxiIngestionJobValidateInputs:
    """Tests for TaxiIngestionJob.validate_inputs method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def teardown_method(self):
        """Reset JobConfig singleton after each test."""
        JobConfig.reset()

    def test_validate_inputs_valid(self):
        """Test validate_inputs passes for valid job."""
        job = TaxiIngestionJob("yellow", 2024, 1)
        # Should not raise
        job.validate_inputs()

    def test_validate_inputs_logs_info(self):
        """Test validate_inputs logs validation info."""
        job = TaxiIngestionJob("yellow", 2024, 1)
        with patch.object(job.logger, 'info') as mock_log:
            job.validate_inputs()
            mock_log.assert_called()


class TestTaxiIngestionJobConstants:
    """Tests for TaxiIngestionJob class constants."""

    def test_nyc_tlc_base_url(self):
        """Test NYC TLC base URL constant."""
        assert "d37ci6vzurychx.cloudfront.net" in TaxiIngestionJob.NYC_TLC_BASE_URL
        assert TaxiIngestionJob.NYC_TLC_BASE_URL.startswith("https://")

    def test_valid_taxi_types(self):
        """Test valid taxi types constant."""
        assert "yellow" in TaxiIngestionJob.VALID_TAXI_TYPES
        assert "green" in TaxiIngestionJob.VALID_TAXI_TYPES
        assert len(TaxiIngestionJob.VALID_TAXI_TYPES) == 2

    def test_min_year(self):
        """Test minimum year constant."""
        assert TaxiIngestionJob.MIN_YEAR == 2009


class TestTaxiIngestionJobExtract:
    """Tests for TaxiIngestionJob.extract method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def teardown_method(self):
        """Reset JobConfig singleton after each test."""
        JobConfig.reset()

    def test_extract_builds_correct_url(self):
        """Test extract builds correct URL."""
        job = TaxiIngestionJob("yellow", 2024, 1)
        expected_url = f"{TaxiIngestionJob.NYC_TLC_BASE_URL}/yellow_tripdata_2024-01.parquet"
        
        with patch.object(job, '_extract_with_local_cache') as mock_extract:
            mock_extract.return_value = Mock()
            job.extract()
            mock_extract.assert_called_once_with(expected_url)


class TestRunIngestionFunction:
    """Tests for run_ingestion convenience function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def teardown_method(self):
        """Reset JobConfig singleton after each test."""
        JobConfig.reset()

    def test_run_ingestion_creates_job(self):
        """Test run_ingestion creates TaxiIngestionJob."""
        with patch.object(TaxiIngestionJob, 'run', return_value=True) as mock_run:
            result = run_ingestion("yellow", 2024, 1)
            mock_run.assert_called_once()
            assert result is True

    def test_run_ingestion_returns_false_on_failure(self):
        """Test run_ingestion returns False on job failure."""
        with patch.object(TaxiIngestionJob, 'run', return_value=False):
            result = run_ingestion("yellow", 2024, 1)
            assert result is False
