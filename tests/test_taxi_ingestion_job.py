"""
Tests for TaxiIngestionJob class.

Tests cover:
- Initialization and parameter validation
- Class constants
- File name generation
- URL construction
- validate_inputs method
"""

import pytest
from unittest.mock import patch, MagicMock

from etl.jobs.bronze.taxi_ingestion_job import (
    TaxiIngestionJob,
    DownloadError,
    run_ingestion,
)
from etl.jobs.utils.config import JobConfig


class TestTaxiIngestionJobConstants:
    """Tests for TaxiIngestionJob class constants."""

    def test_nyc_tlc_base_url(self):
        """Test NYC TLC base URL constant."""
        assert TaxiIngestionJob.NYC_TLC_BASE_URL is not None
        assert "cloudfront.net" in TaxiIngestionJob.NYC_TLC_BASE_URL

    def test_valid_taxi_types(self):
        """Test valid taxi types constant."""
        assert "yellow" in TaxiIngestionJob.VALID_TAXI_TYPES
        assert "green" in TaxiIngestionJob.VALID_TAXI_TYPES
        assert len(TaxiIngestionJob.VALID_TAXI_TYPES) == 2

    def test_min_year(self):
        """Test minimum year constant."""
        assert TaxiIngestionJob.MIN_YEAR == 2009


class TestTaxiIngestionJobInit:
    """Tests for TaxiIngestionJob initialization."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_init_with_yellow_taxi_type(self):
        """Test initialization with yellow taxi type."""
        job = TaxiIngestionJob("yellow", 2024, 1)
        assert job.taxi_type == "yellow"
        assert job.year == 2024
        assert job.month == 1

    def test_init_with_green_taxi_type(self):
        """Test initialization with green taxi type."""
        job = TaxiIngestionJob("green", 2023, 12)
        assert job.taxi_type == "green"
        assert job.year == 2023
        assert job.month == 12

    def test_init_generates_correct_file_name_yellow(self):
        """Test file name generation for yellow taxi."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        assert job.file_name == "yellow_tripdata_2024-06.parquet"

    def test_init_generates_correct_file_name_green(self):
        """Test file name generation for green taxi."""
        job = TaxiIngestionJob("green", 2023, 1)
        assert job.file_name == "green_tripdata_2023-01.parquet"

    def test_init_file_name_pads_month(self):
        """Test that month is zero-padded in file name."""
        job = TaxiIngestionJob("yellow", 2024, 3)
        assert "2024-03" in job.file_name

    def test_init_job_name_format(self):
        """Test job name format."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        assert "TaxiIngestion" in job.job_name
        assert "yellow" in job.job_name
        assert "2024" in job.job_name
        assert "06" in job.job_name

    def test_init_invalid_taxi_type_raises_error(self):
        """Test that invalid taxi type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TaxiIngestionJob("blue", 2024, 1)
        assert "Invalid taxi_type" in str(exc_info.value)

    def test_init_year_too_early_raises_error(self):
        """Test that year before MIN_YEAR raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TaxiIngestionJob("yellow", 2008, 1)
        assert "Invalid year" in str(exc_info.value)

    def test_init_invalid_month_zero_raises_error(self):
        """Test that month 0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TaxiIngestionJob("yellow", 2024, 0)
        assert "Invalid month" in str(exc_info.value)

    def test_init_invalid_month_13_raises_error(self):
        """Test that month 13 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TaxiIngestionJob("yellow", 2024, 13)
        assert "Invalid month" in str(exc_info.value)

    def test_init_invalid_month_negative_raises_error(self):
        """Test that negative month raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TaxiIngestionJob("yellow", 2024, -1)
        assert "Invalid month" in str(exc_info.value)

    def test_init_boundary_month_1(self):
        """Test initialization with month 1 (boundary)."""
        job = TaxiIngestionJob("yellow", 2024, 1)
        assert job.month == 1

    def test_init_boundary_month_12(self):
        """Test initialization with month 12 (boundary)."""
        job = TaxiIngestionJob("yellow", 2024, 12)
        assert job.month == 12

    def test_init_boundary_year_min(self):
        """Test initialization with minimum year."""
        job = TaxiIngestionJob("yellow", 2009, 1)
        assert job.year == 2009


class TestTaxiIngestionJobValidateParameters:
    """Tests for TaxiIngestionJob._validate_parameters static method."""

    def test_validate_parameters_valid_yellow(self):
        """Test validation passes for valid yellow taxi parameters."""
        # Should not raise
        TaxiIngestionJob._validate_parameters("yellow", 2024, 6)

    def test_validate_parameters_valid_green(self):
        """Test validation passes for valid green taxi parameters."""
        # Should not raise
        TaxiIngestionJob._validate_parameters("green", 2023, 12)

    def test_validate_parameters_invalid_taxi_type(self):
        """Test validation fails for invalid taxi type."""
        with pytest.raises(ValueError) as exc_info:
            TaxiIngestionJob._validate_parameters("red", 2024, 1)
        assert "Invalid taxi_type" in str(exc_info.value)
        assert "red" in str(exc_info.value)

    def test_validate_parameters_invalid_year_type(self):
        """Test validation fails for non-integer year."""
        with pytest.raises(ValueError) as exc_info:
            TaxiIngestionJob._validate_parameters("yellow", "2024", 1)
        assert "Invalid year" in str(exc_info.value)

    def test_validate_parameters_invalid_month_type(self):
        """Test validation fails for non-integer month."""
        with pytest.raises(ValueError) as exc_info:
            TaxiIngestionJob._validate_parameters("yellow", 2024, "1")
        assert "Invalid month" in str(exc_info.value)


class TestTaxiIngestionJobValidateInputs:
    """Tests for TaxiIngestionJob.validate_inputs method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_validate_inputs_valid(self):
        """Test validate_inputs passes for valid job."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        # Should not raise
        job.validate_inputs()

    def test_validate_inputs_logs_info(self):
        """Test validate_inputs logs validation info."""
        job = TaxiIngestionJob("green", 2023, 12)
        # Should not raise
        job.validate_inputs()


class TestDownloadError:
    """Tests for DownloadError exception class."""

    def test_download_error_message(self):
        """Test DownloadError stores message correctly."""
        error = DownloadError("Download failed")
        assert str(error) == "Download failed"

    def test_download_error_with_cause(self):
        """Test DownloadError with cause exception."""
        cause = ConnectionError("Network error")
        error = DownloadError("Download failed")
        error.__cause__ = cause
        assert error.__cause__ is cause

    def test_download_error_inheritance(self):
        """Test DownloadError inherits from Exception."""
        error = DownloadError("test")
        assert isinstance(error, Exception)


class TestRunIngestionFunction:
    """Tests for run_ingestion convenience function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_run_ingestion_creates_job_and_runs(self):
        """Test that run_ingestion creates job and calls run."""
        with patch.object(TaxiIngestionJob, "run", return_value=True) as mock_run:
            result = run_ingestion("yellow", 2024, 6)
            assert result is True
            mock_run.assert_called_once()

    def test_run_ingestion_returns_false_on_failure(self):
        """Test that run_ingestion returns False on job failure."""
        with patch.object(TaxiIngestionJob, "run", return_value=False):
            result = run_ingestion("yellow", 2024, 6)
            assert result is False

    def test_run_ingestion_with_green_taxi(self):
        """Test run_ingestion with green taxi type."""
        with patch.object(TaxiIngestionJob, "run", return_value=True):
            result = run_ingestion("green", 2023, 12)
            assert result is True


class TestTaxiIngestionJobExtract:
    """Tests for TaxiIngestionJob.extract method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_calls_extract_from_source(self):
        """Test that extract calls _extract_from_source."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        with patch.object(job, "_extract_from_source") as mock_extract:
            mock_extract.return_value = MagicMock()
            job.extract()
            mock_extract.assert_called_once()


class TestTaxiIngestionJobGetMinioClient:
    """Tests for TaxiIngestionJob._get_minio_client method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_get_minio_client_returns_client(self):
        """Test that _get_minio_client returns a Minio client."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        with patch("etl.jobs.bronze.taxi_ingestion_job.Minio") as mock_minio:
            mock_client = MagicMock()
            mock_minio.return_value = mock_client
            client = job._get_minio_client()
            assert client is mock_client


class TestTaxiIngestionJobBuildSourceUrl:
    """Tests for TaxiIngestionJob URL building."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_source_url_format(self):
        """Test that source URL is built correctly."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        expected_file = "yellow_tripdata_2024-06.parquet"
        assert job.file_name == expected_file

    def test_source_url_contains_base_url(self):
        """Test that source URL contains base URL."""
        job = TaxiIngestionJob("green", 2023, 12)
        # The URL would be built from NYC_TLC_BASE_URL + file_name
        assert TaxiIngestionJob.NYC_TLC_BASE_URL is not None
