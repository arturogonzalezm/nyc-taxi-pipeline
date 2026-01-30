"""
Tests for TaxiGoldJob class.

Tests cover:
- Initialization and parameter validation
- Class constants
- Date range handling
- validate_inputs method
"""

import pytest
from unittest.mock import patch

from etl.jobs.gold.taxi_gold_job import TaxiGoldJob, run_gold_job
from etl.jobs.utils.config import JobConfig


class TestTaxiGoldJobConstants:
    """Tests for TaxiGoldJob class constants."""

    def test_valid_taxi_types(self):
        """Test valid taxi types constant."""
        assert "yellow" in TaxiGoldJob.VALID_TAXI_TYPES
        assert "green" in TaxiGoldJob.VALID_TAXI_TYPES
        assert len(TaxiGoldJob.VALID_TAXI_TYPES) == 2

    def test_min_year(self):
        """Test minimum year constant."""
        assert TaxiGoldJob.MIN_YEAR == 2009


class TestTaxiGoldJobInit:
    """Tests for TaxiGoldJob initialization."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_init_with_yellow_taxi_type(self):
        """Test initialization with yellow taxi type."""
        job = TaxiGoldJob("yellow", 2024, 1)
        assert job.taxi_type == "yellow"
        assert job.year == 2024
        assert job.month == 1

    def test_init_with_green_taxi_type(self):
        """Test initialization with green taxi type."""
        job = TaxiGoldJob("green", 2023, 12)
        assert job.taxi_type == "green"
        assert job.year == 2023
        assert job.month == 12

    def test_init_with_date_range(self):
        """Test initialization with date range."""
        job = TaxiGoldJob("yellow", 2023, 1, end_year=2023, end_month=6)
        assert job.year == 2023
        assert job.month == 1
        assert job.end_year == 2023
        assert job.end_month == 6

    def test_init_without_end_date_uses_start_date(self):
        """Test that end date defaults to start date when not specified."""
        job = TaxiGoldJob("yellow", 2024, 6)
        assert job.end_year == 2024
        assert job.end_month == 6

    def test_init_job_name_format(self):
        """Test job name format."""
        job = TaxiGoldJob("yellow", 2024, 6)
        assert "TaxiGold" in job.job_name
        assert "yellow" in job.job_name
        assert "2024" in job.job_name
        assert "06" in job.job_name

    def test_init_invalid_taxi_type_raises_error(self):
        """Test that invalid taxi type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TaxiGoldJob("blue", 2024, 1)
        assert "Invalid taxi_type" in str(exc_info.value)

    def test_init_year_too_early_raises_error(self):
        """Test that year before MIN_YEAR raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TaxiGoldJob("yellow", 2008, 1)
        assert "Invalid year" in str(exc_info.value)

    def test_init_invalid_month_zero_raises_error(self):
        """Test that month 0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TaxiGoldJob("yellow", 2024, 0)
        assert "Invalid month" in str(exc_info.value)

    def test_init_invalid_month_13_raises_error(self):
        """Test that month 13 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TaxiGoldJob("yellow", 2024, 13)
        assert "Invalid month" in str(exc_info.value)

    def test_init_end_date_before_start_raises_error(self):
        """Test that end date before start date raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TaxiGoldJob("yellow", 2024, 6, end_year=2024, end_month=3)
        assert "End date must be >= start date" in str(exc_info.value)

    def test_init_end_year_before_start_year_raises_error(self):
        """Test that end year before start year raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TaxiGoldJob("yellow", 2024, 1, end_year=2023, end_month=12)
        assert "End date must be >= start date" in str(exc_info.value)

    def test_init_boundary_month_1(self):
        """Test initialization with month 1 (boundary)."""
        job = TaxiGoldJob("yellow", 2024, 1)
        assert job.month == 1

    def test_init_boundary_month_12(self):
        """Test initialization with month 12 (boundary)."""
        job = TaxiGoldJob("yellow", 2024, 12)
        assert job.month == 12

    def test_init_boundary_year_min(self):
        """Test initialization with minimum year."""
        job = TaxiGoldJob("yellow", 2009, 1)
        assert job.year == 2009

    def test_init_cross_year_date_range(self):
        """Test initialization with cross-year date range."""
        job = TaxiGoldJob("yellow", 2023, 11, end_year=2024, end_month=2)
        assert job.year == 2023
        assert job.month == 11
        assert job.end_year == 2024
        assert job.end_month == 2


class TestTaxiGoldJobValidateParameters:
    """Tests for TaxiGoldJob._validate_parameters static method."""

    def test_validate_parameters_valid_yellow(self):
        """Test validation passes for valid yellow taxi parameters."""
        # Should not raise
        TaxiGoldJob._validate_parameters("yellow", 2024, 6, None, None)

    def test_validate_parameters_valid_green(self):
        """Test validation passes for valid green taxi parameters."""
        # Should not raise
        TaxiGoldJob._validate_parameters("green", 2023, 12, None, None)

    def test_validate_parameters_valid_date_range(self):
        """Test validation passes for valid date range."""
        # Should not raise
        TaxiGoldJob._validate_parameters("yellow", 2023, 1, 2023, 12)

    def test_validate_parameters_invalid_taxi_type(self):
        """Test validation fails for invalid taxi type."""
        with pytest.raises(ValueError) as exc_info:
            TaxiGoldJob._validate_parameters("red", 2024, 1, None, None)
        assert "Invalid taxi_type" in str(exc_info.value)

    def test_validate_parameters_invalid_year_type(self):
        """Test validation fails for non-integer year."""
        with pytest.raises(ValueError) as exc_info:
            TaxiGoldJob._validate_parameters("yellow", "2024", 1, None, None)
        assert "Invalid year" in str(exc_info.value)

    def test_validate_parameters_invalid_month_type(self):
        """Test validation fails for non-integer month."""
        with pytest.raises(ValueError) as exc_info:
            TaxiGoldJob._validate_parameters("yellow", 2024, "1", None, None)
        assert "Invalid month" in str(exc_info.value)

    def test_validate_parameters_end_before_start(self):
        """Test validation fails when end date is before start date."""
        with pytest.raises(ValueError) as exc_info:
            TaxiGoldJob._validate_parameters("yellow", 2024, 6, 2024, 3)
        assert "End date must be >= start date" in str(exc_info.value)


class TestTaxiGoldJobValidateInputs:
    """Tests for TaxiGoldJob.validate_inputs method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_validate_inputs_valid(self):
        """Test validate_inputs passes for valid job."""
        job = TaxiGoldJob("yellow", 2024, 6)
        # Should not raise
        job.validate_inputs()

    def test_validate_inputs_with_date_range(self):
        """Test validate_inputs passes for job with date range."""
        job = TaxiGoldJob("yellow", 2023, 1, end_year=2023, end_month=12)
        # Should not raise
        job.validate_inputs()


class TestRunGoldJobFunction:
    """Tests for run_gold_job convenience function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_run_gold_job_creates_job_and_runs(self):
        """Test that run_gold_job creates job and calls run."""
        with patch.object(TaxiGoldJob, "run", return_value=True) as mock_run:
            result = run_gold_job("yellow", 2024, 6)
            assert result is True
            mock_run.assert_called_once()

    def test_run_gold_job_returns_false_on_failure(self):
        """Test that run_gold_job returns False on job failure."""
        with patch.object(TaxiGoldJob, "run", return_value=False):
            result = run_gold_job("yellow", 2024, 6)
            assert result is False

    def test_run_gold_job_with_date_range(self):
        """Test run_gold_job with date range."""
        with patch.object(TaxiGoldJob, "run", return_value=True):
            result = run_gold_job("yellow", 2023, 1, end_year=2023, end_month=6)
            assert result is True

    def test_run_gold_job_with_green_taxi(self):
        """Test run_gold_job with green taxi type."""
        with patch.object(TaxiGoldJob, "run", return_value=True):
            result = run_gold_job("green", 2023, 12)
            assert result is True
