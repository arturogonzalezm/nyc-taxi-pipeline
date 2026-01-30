"""
Extended tests for TaxiIngestionJob to improve coverage.
"""

import pytest
from unittest.mock import patch, MagicMock

from etl.jobs.bronze.taxi_ingestion_job import (
    TaxiIngestionJob,
    DownloadError,
    run_ingestion,
)
from etl.jobs.base_job import BaseSparkJob
from etl.jobs.utils.config import JobConfig


class TestTaxiIngestionJobInheritance:
    """Tests for TaxiIngestionJob inheritance."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_inherits_from_base_spark_job(self):
        """Test TaxiIngestionJob inherits from BaseSparkJob."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        assert isinstance(job, BaseSparkJob)

    def test_has_logger(self):
        """Test job has logger."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        assert job.logger is not None

    def test_has_config(self):
        """Test job has config."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        assert job.config is not None


class TestTaxiIngestionJobAttributes:
    """Tests for TaxiIngestionJob attributes."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_taxi_type_yellow(self):
        """Test taxi_type attribute for yellow."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        assert job.taxi_type == "yellow"

    def test_taxi_type_green(self):
        """Test taxi_type attribute for green."""
        job = TaxiIngestionJob("green", 2024, 6)
        assert job.taxi_type == "green"

    def test_year_attribute(self):
        """Test year attribute."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        assert job.year == 2024

    def test_month_attribute(self):
        """Test month attribute."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        assert job.month == 6

    def test_file_name_yellow(self):
        """Test file_name for yellow taxi."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        assert job.file_name == "yellow_tripdata_2024-06.parquet"

    def test_file_name_green(self):
        """Test file_name for green taxi."""
        job = TaxiIngestionJob("green", 2023, 12)
        assert job.file_name == "green_tripdata_2023-12.parquet"

    def test_file_name_month_padding(self):
        """Test file_name has zero-padded month."""
        job = TaxiIngestionJob("yellow", 2024, 1)
        assert "2024-01" in job.file_name

    def test_job_name_format(self):
        """Test job_name format."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        assert "TaxiIngestion" in job.job_name
        assert "yellow" in job.job_name


class TestTaxiIngestionJobValidation:
    """Tests for TaxiIngestionJob validation."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_invalid_taxi_type_red(self):
        """Test invalid taxi type 'red'."""
        with pytest.raises(ValueError) as exc_info:
            TaxiIngestionJob("red", 2024, 6)
        assert "Invalid taxi_type" in str(exc_info.value)

    def test_invalid_taxi_type_empty(self):
        """Test empty taxi type."""
        with pytest.raises(ValueError):
            TaxiIngestionJob("", 2024, 6)

    def test_invalid_year_2000(self):
        """Test year 2000 is invalid."""
        with pytest.raises(ValueError):
            TaxiIngestionJob("yellow", 2000, 6)

    def test_invalid_year_negative(self):
        """Test negative year is invalid."""
        with pytest.raises(ValueError):
            TaxiIngestionJob("yellow", -2024, 6)

    def test_valid_year_2009(self):
        """Test year 2009 is valid (MIN_YEAR)."""
        job = TaxiIngestionJob("yellow", 2009, 1)
        assert job.year == 2009

    def test_valid_year_2024(self):
        """Test year 2024 is valid."""
        job = TaxiIngestionJob("yellow", 2024, 1)
        assert job.year == 2024

    def test_invalid_month_0(self):
        """Test month 0 is invalid."""
        with pytest.raises(ValueError):
            TaxiIngestionJob("yellow", 2024, 0)

    def test_invalid_month_13(self):
        """Test month 13 is invalid."""
        with pytest.raises(ValueError):
            TaxiIngestionJob("yellow", 2024, 13)

    def test_invalid_month_negative(self):
        """Test negative month is invalid."""
        with pytest.raises(ValueError):
            TaxiIngestionJob("yellow", 2024, -1)

    def test_valid_month_1(self):
        """Test month 1 is valid."""
        job = TaxiIngestionJob("yellow", 2024, 1)
        assert job.month == 1

    def test_valid_month_12(self):
        """Test month 12 is valid."""
        job = TaxiIngestionJob("yellow", 2024, 12)
        assert job.month == 12


class TestTaxiIngestionJobValidateInputsMethod:
    """Tests for TaxiIngestionJob.validate_inputs method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_validate_inputs_yellow(self):
        """Test validate_inputs for yellow taxi."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        job.validate_inputs()  # Should not raise

    def test_validate_inputs_green(self):
        """Test validate_inputs for green taxi."""
        job = TaxiIngestionJob("green", 2023, 12)
        job.validate_inputs()  # Should not raise

    def test_validate_inputs_boundary_month(self):
        """Test validate_inputs with boundary months."""
        job1 = TaxiIngestionJob("yellow", 2024, 1)
        job1.validate_inputs()  # Should not raise

        job2 = TaxiIngestionJob("yellow", 2024, 12)
        job2.validate_inputs()  # Should not raise


class TestTaxiIngestionJobExtractMethod:
    """Tests for TaxiIngestionJob.extract method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_calls_extract_from_source(self):
        """Test extract calls _extract_from_source."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        with patch.object(job, "_extract_from_source") as mock_extract:
            mock_df = MagicMock()
            mock_extract.return_value = mock_df
            result = job.extract()
            mock_extract.assert_called_once()
            assert result is mock_df


class TestTaxiIngestionJobConstants:
    """Tests for TaxiIngestionJob constants."""

    def test_nyc_tlc_base_url_is_cloudfront(self):
        """Test NYC_TLC_BASE_URL is CloudFront."""
        assert "cloudfront.net" in TaxiIngestionJob.NYC_TLC_BASE_URL

    def test_nyc_tlc_base_url_is_https(self):
        """Test NYC_TLC_BASE_URL uses HTTPS."""
        assert TaxiIngestionJob.NYC_TLC_BASE_URL.startswith("https://")

    def test_valid_taxi_types_yellow(self):
        """Test VALID_TAXI_TYPES contains yellow."""
        assert "yellow" in TaxiIngestionJob.VALID_TAXI_TYPES

    def test_valid_taxi_types_green(self):
        """Test VALID_TAXI_TYPES contains green."""
        assert "green" in TaxiIngestionJob.VALID_TAXI_TYPES

    def test_valid_taxi_types_count(self):
        """Test VALID_TAXI_TYPES has 2 types."""
        assert len(TaxiIngestionJob.VALID_TAXI_TYPES) == 2

    def test_min_year_is_2009(self):
        """Test MIN_YEAR is 2009."""
        assert TaxiIngestionJob.MIN_YEAR == 2009


class TestRunIngestionFunction:
    """Tests for run_ingestion convenience function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_run_ingestion_success(self):
        """Test run_ingestion returns True on success."""
        with patch.object(TaxiIngestionJob, "run", return_value=True):
            result = run_ingestion("yellow", 2024, 6)
            assert result is True

    def test_run_ingestion_failure(self):
        """Test run_ingestion returns False on failure."""
        with patch.object(TaxiIngestionJob, "run", return_value=False):
            result = run_ingestion("yellow", 2024, 6)
            assert result is False

    def test_run_ingestion_green_taxi(self):
        """Test run_ingestion with green taxi."""
        with patch.object(TaxiIngestionJob, "run", return_value=True):
            result = run_ingestion("green", 2023, 12)
            assert result is True

    def test_run_ingestion_creates_job(self):
        """Test run_ingestion creates TaxiIngestionJob."""
        with patch.object(TaxiIngestionJob, "run", return_value=True) as mock_run:
            run_ingestion("yellow", 2024, 6)
            mock_run.assert_called_once()


class TestDownloadErrorClass:
    """Tests for DownloadError exception class."""

    def test_download_error_message(self):
        """Test DownloadError message."""
        error = DownloadError("Failed to download file")
        assert "Failed to download file" in str(error)

    def test_download_error_is_exception(self):
        """Test DownloadError is an Exception."""
        error = DownloadError("test")
        assert isinstance(error, Exception)

    def test_download_error_with_url(self):
        """Test DownloadError with URL."""
        url = "https://example.com/file.parquet"
        error = DownloadError(f"Failed: {url}")
        assert url in str(error)

    def test_download_error_with_status(self):
        """Test DownloadError with HTTP status."""
        error = DownloadError("HTTP 404 Not Found")
        assert "404" in str(error)

    def test_download_error_can_be_raised(self):
        """Test DownloadError can be raised and caught."""
        with pytest.raises(DownloadError):
            raise DownloadError("Test error")

    def test_download_error_with_cause(self):
        """Test DownloadError with cause."""
        cause = ConnectionError("Network error")
        try:
            raise DownloadError("Download failed") from cause
        except DownloadError as e:
            assert e.__cause__ is cause
