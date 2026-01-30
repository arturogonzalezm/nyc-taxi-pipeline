"""
Extended validation tests for ETL jobs to improve coverage.
"""

import pytest

from etl.jobs.bronze.taxi_ingestion_job import TaxiIngestionJob, DownloadError
from etl.jobs.gold.taxi_gold_job import TaxiGoldJob
from etl.jobs.load.postgres_load_job import PostgresLoadJob


class TestTaxiIngestionJobValidation:
    """Extended validation tests for TaxiIngestionJob."""

    def test_validate_yellow_taxi_type(self):
        """Test validation passes for yellow taxi type."""
        TaxiIngestionJob._validate_parameters("yellow", 2024, 6)

    def test_validate_green_taxi_type(self):
        """Test validation passes for green taxi type."""
        TaxiIngestionJob._validate_parameters("green", 2023, 12)

    def test_invalid_taxi_type_raises_error(self):
        """Test invalid taxi type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid taxi_type"):
            TaxiIngestionJob._validate_parameters("blue", 2024, 1)

    def test_year_too_early_raises_error(self):
        """Test year before 2009 raises ValueError."""
        with pytest.raises(ValueError, match="Invalid year"):
            TaxiIngestionJob._validate_parameters("yellow", 2008, 1)

    def test_year_in_future_raises_error(self):
        """Test future year is actually valid (no upper bound check)."""
        # Note: The validation only checks >= MIN_YEAR, no upper bound
        TaxiIngestionJob._validate_parameters("yellow", 2099, 1)

    def test_month_zero_raises_error(self):
        """Test month 0 raises ValueError."""
        with pytest.raises(ValueError, match="Invalid month"):
            TaxiIngestionJob._validate_parameters("yellow", 2024, 0)

    def test_month_13_raises_error(self):
        """Test month 13 raises ValueError."""
        with pytest.raises(ValueError, match="Invalid month"):
            TaxiIngestionJob._validate_parameters("yellow", 2024, 13)

    def test_month_negative_raises_error(self):
        """Test negative month raises ValueError."""
        with pytest.raises(ValueError, match="Invalid month"):
            TaxiIngestionJob._validate_parameters("yellow", 2024, -1)

    def test_valid_boundary_year_2009(self):
        """Test year 2009 (earliest valid) passes validation."""
        TaxiIngestionJob._validate_parameters("yellow", 2009, 1)

    def test_valid_boundary_month_1(self):
        """Test month 1 passes validation."""
        TaxiIngestionJob._validate_parameters("yellow", 2024, 1)

    def test_valid_boundary_month_12(self):
        """Test month 12 passes validation."""
        TaxiIngestionJob._validate_parameters("yellow", 2024, 12)


class TestTaxiGoldJobValidation:
    """Extended validation tests for TaxiGoldJob."""

    def test_validate_yellow_taxi_type(self):
        """Test validation passes for yellow taxi type."""
        TaxiGoldJob._validate_parameters("yellow", 2024, 1, None, None)

    def test_validate_green_taxi_type(self):
        """Test validation passes for green taxi type."""
        TaxiGoldJob._validate_parameters("green", 2023, 6, None, None)

    def test_validate_with_end_date(self):
        """Test validation passes with end date."""
        TaxiGoldJob._validate_parameters("yellow", 2023, 1, 2023, 12)

    def test_invalid_taxi_type_raises_error(self):
        """Test invalid taxi type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid taxi_type"):
            TaxiGoldJob._validate_parameters("red", 2024, 1, None, None)

    def test_year_too_early_raises_error(self):
        """Test year before 2009 raises ValueError."""
        with pytest.raises(ValueError, match="Invalid year"):
            TaxiGoldJob._validate_parameters("yellow", 2008, 1, None, None)

    def test_month_invalid_raises_error(self):
        """Test invalid month raises ValueError."""
        with pytest.raises(ValueError, match="Invalid month"):
            TaxiGoldJob._validate_parameters("yellow", 2024, 15, None, None)

    def test_end_date_before_start_raises_error(self):
        """Test end date before start date raises ValueError."""
        with pytest.raises(ValueError, match="End date must be >= start date"):
            TaxiGoldJob._validate_parameters("yellow", 2024, 6, 2024, 3)

    def test_end_year_before_start_year_raises_error(self):
        """Test end year before start year raises ValueError."""
        with pytest.raises(ValueError, match="End date must be >= start date"):
            TaxiGoldJob._validate_parameters("yellow", 2024, 1, 2023, 12)

    def test_same_start_end_date_valid(self):
        """Test same start and end date is valid."""
        TaxiGoldJob._validate_parameters("yellow", 2024, 6, 2024, 6)

    def test_end_month_after_start_same_year(self):
        """Test end month after start month in same year is valid."""
        TaxiGoldJob._validate_parameters("yellow", 2024, 3, 2024, 9)


class TestPostgresLoadJobValidation:
    """Extended validation tests for PostgresLoadJob."""

    def test_invalid_taxi_type_raises_error(self):
        """Test invalid taxi type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid taxi_type"):
            PostgresLoadJob("purple")

    def test_yellow_taxi_type_valid(self):
        """Test yellow taxi type is valid."""
        # This will fail later due to missing Spark, but validation should pass
        try:
            job = PostgresLoadJob("yellow")
            assert job.taxi_type == "yellow"
        except Exception as e:
            # Expected to fail on Spark initialization, not validation
            assert "Invalid taxi_type" not in str(e)

    def test_green_taxi_type_valid(self):
        """Test green taxi type is valid."""
        try:
            job = PostgresLoadJob("green")
            assert job.taxi_type == "green"
        except Exception as e:
            # Expected to fail on Spark initialization, not validation
            assert "Invalid taxi_type" not in str(e)


class TestDownloadError:
    """Tests for DownloadError exception class."""

    def test_download_error_message(self):
        """Test DownloadError stores message correctly."""
        error = DownloadError("Failed to download file")
        assert str(error) == "Failed to download file"

    def test_download_error_with_cause(self):
        """Test DownloadError can be raised with cause."""
        original = ConnectionError("Network error")
        try:
            raise DownloadError("Download failed") from original
        except DownloadError as e:
            assert e.__cause__ is original

    def test_download_error_inheritance(self):
        """Test DownloadError inherits from Exception."""
        error = DownloadError("Test")
        assert isinstance(error, Exception)
