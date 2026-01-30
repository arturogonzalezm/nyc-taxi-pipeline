"""
Extended tests for TaxiGoldJob to improve coverage.
"""

import pytest
from unittest.mock import patch, MagicMock

from etl.jobs.gold.taxi_gold_job import TaxiGoldJob, run_gold_job
from etl.jobs.base_job import BaseSparkJob
from etl.jobs.utils.config import JobConfig


class TestTaxiGoldJobInheritance:
    """Tests for TaxiGoldJob inheritance."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_inherits_from_base_spark_job(self):
        """Test TaxiGoldJob inherits from BaseSparkJob."""
        job = TaxiGoldJob("yellow", 2024, 6)
        assert isinstance(job, BaseSparkJob)

    def test_has_logger(self):
        """Test job has logger."""
        job = TaxiGoldJob("yellow", 2024, 6)
        assert job.logger is not None

    def test_has_config(self):
        """Test job has config."""
        job = TaxiGoldJob("yellow", 2024, 6)
        assert job.config is not None


class TestTaxiGoldJobAttributes:
    """Tests for TaxiGoldJob attributes."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_taxi_type_attribute(self):
        """Test taxi_type attribute is set."""
        job = TaxiGoldJob("yellow", 2024, 6)
        assert job.taxi_type == "yellow"

    def test_year_attribute(self):
        """Test year attribute is set."""
        job = TaxiGoldJob("yellow", 2024, 6)
        assert job.year == 2024

    def test_month_attribute(self):
        """Test month attribute is set."""
        job = TaxiGoldJob("yellow", 2024, 6)
        assert job.month == 6

    def test_end_year_attribute_default(self):
        """Test end_year defaults to year."""
        job = TaxiGoldJob("yellow", 2024, 6)
        assert job.end_year == 2024

    def test_end_month_attribute_default(self):
        """Test end_month defaults to month."""
        job = TaxiGoldJob("yellow", 2024, 6)
        assert job.end_month == 6

    def test_end_year_attribute_custom(self):
        """Test end_year with custom value."""
        job = TaxiGoldJob("yellow", 2024, 1, end_year=2024, end_month=12)
        assert job.end_year == 2024

    def test_end_month_attribute_custom(self):
        """Test end_month with custom value."""
        job = TaxiGoldJob("yellow", 2024, 1, end_year=2024, end_month=12)
        assert job.end_month == 12


class TestTaxiGoldJobValidation:
    """Tests for TaxiGoldJob validation."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_invalid_taxi_type_orange(self):
        """Test invalid taxi type 'orange' raises error."""
        with pytest.raises(ValueError) as exc_info:
            TaxiGoldJob("orange", 2024, 6)
        assert "Invalid taxi_type" in str(exc_info.value)

    def test_invalid_taxi_type_empty(self):
        """Test empty taxi type raises error."""
        with pytest.raises(ValueError):
            TaxiGoldJob("", 2024, 6)

    def test_invalid_year_string(self):
        """Test string year raises error."""
        with pytest.raises(ValueError):
            TaxiGoldJob("yellow", "2024", 6)

    def test_invalid_month_string(self):
        """Test string month raises error."""
        with pytest.raises(ValueError):
            TaxiGoldJob("yellow", 2024, "6")

    def test_year_2008_invalid(self):
        """Test year 2008 is invalid (before MIN_YEAR)."""
        with pytest.raises(ValueError):
            TaxiGoldJob("yellow", 2008, 6)

    def test_year_2009_valid(self):
        """Test year 2009 is valid (MIN_YEAR)."""
        job = TaxiGoldJob("yellow", 2009, 6)
        assert job.year == 2009

    def test_month_0_invalid(self):
        """Test month 0 is invalid."""
        with pytest.raises(ValueError):
            TaxiGoldJob("yellow", 2024, 0)

    def test_month_13_invalid(self):
        """Test month 13 is invalid."""
        with pytest.raises(ValueError):
            TaxiGoldJob("yellow", 2024, 13)

    def test_end_date_before_start_same_year(self):
        """Test end date before start date in same year."""
        with pytest.raises(ValueError) as exc_info:
            TaxiGoldJob("yellow", 2024, 6, end_year=2024, end_month=3)
        assert "End date must be >= start date" in str(exc_info.value)

    def test_end_year_before_start_year(self):
        """Test end year before start year."""
        with pytest.raises(ValueError):
            TaxiGoldJob("yellow", 2024, 6, end_year=2023, end_month=12)


class TestTaxiGoldJobExtractMethod:
    """Tests for TaxiGoldJob.extract method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_returns_tuple(self):
        """Test extract returns a tuple."""
        job = TaxiGoldJob("yellow", 2024, 6)
        with patch.object(job, "_extract_bronze_trips") as mock_trips:
            with patch.object(job, "_extract_zone_lookup") as mock_zones:
                mock_trips.return_value = MagicMock()
                mock_zones.return_value = MagicMock()
                result = job.extract()
                assert isinstance(result, tuple)
                assert len(result) == 2

    def test_extract_calls_bronze_trips(self):
        """Test extract calls _extract_bronze_trips."""
        job = TaxiGoldJob("yellow", 2024, 6)
        with patch.object(job, "_extract_bronze_trips") as mock_trips:
            with patch.object(job, "_extract_zone_lookup") as mock_zones:
                mock_trips.return_value = MagicMock()
                mock_zones.return_value = MagicMock()
                job.extract()
                mock_trips.assert_called_once()

    def test_extract_calls_zone_lookup(self):
        """Test extract calls _extract_zone_lookup."""
        job = TaxiGoldJob("yellow", 2024, 6)
        with patch.object(job, "_extract_bronze_trips") as mock_trips:
            with patch.object(job, "_extract_zone_lookup") as mock_zones:
                mock_trips.return_value = MagicMock()
                mock_zones.return_value = MagicMock()
                job.extract()
                mock_zones.assert_called_once()


class TestTaxiGoldJobValidateInputsMethod:
    """Tests for TaxiGoldJob.validate_inputs method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_validate_inputs_does_not_raise(self):
        """Test validate_inputs does not raise for valid job."""
        job = TaxiGoldJob("yellow", 2024, 6)
        job.validate_inputs()  # Should not raise

    def test_validate_inputs_with_date_range(self):
        """Test validate_inputs with date range."""
        job = TaxiGoldJob("green", 2023, 1, end_year=2023, end_month=12)
        job.validate_inputs()  # Should not raise

    def test_validate_inputs_cross_year(self):
        """Test validate_inputs with cross-year range."""
        job = TaxiGoldJob("yellow", 2023, 11, end_year=2024, end_month=2)
        job.validate_inputs()  # Should not raise


class TestRunGoldJobFunction:
    """Tests for run_gold_job convenience function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_run_gold_job_success(self):
        """Test run_gold_job returns True on success."""
        with patch.object(TaxiGoldJob, "run", return_value=True):
            result = run_gold_job("yellow", 2024, 6)
            assert result is True

    def test_run_gold_job_failure(self):
        """Test run_gold_job returns False on failure."""
        with patch.object(TaxiGoldJob, "run", return_value=False):
            result = run_gold_job("yellow", 2024, 6)
            assert result is False

    def test_run_gold_job_with_end_date(self):
        """Test run_gold_job with end date parameters."""
        with patch.object(TaxiGoldJob, "run", return_value=True):
            result = run_gold_job("yellow", 2024, 1, end_year=2024, end_month=6)
            assert result is True

    def test_run_gold_job_green_taxi(self):
        """Test run_gold_job with green taxi."""
        with patch.object(TaxiGoldJob, "run", return_value=True):
            result = run_gold_job("green", 2023, 12)
            assert result is True


class TestTaxiGoldJobConstants:
    """Tests for TaxiGoldJob constants."""

    def test_valid_taxi_types_contains_yellow(self):
        """Test VALID_TAXI_TYPES contains yellow."""
        assert "yellow" in TaxiGoldJob.VALID_TAXI_TYPES

    def test_valid_taxi_types_contains_green(self):
        """Test VALID_TAXI_TYPES contains green."""
        assert "green" in TaxiGoldJob.VALID_TAXI_TYPES

    def test_valid_taxi_types_length(self):
        """Test VALID_TAXI_TYPES has exactly 2 types."""
        assert len(TaxiGoldJob.VALID_TAXI_TYPES) == 2

    def test_min_year_is_2009(self):
        """Test MIN_YEAR is 2009."""
        assert TaxiGoldJob.MIN_YEAR == 2009
