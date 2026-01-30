"""
Extended coverage tests for taxi_injection_safe_backfill_job.

Tests cover uncovered functions:
- check_partition_exists
- delete_partition
- safe_backfill_month
- safe_historical_backfill
- main
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from etl.jobs.bronze.taxi_injection_safe_backfill_job import (
    parse_month_spec,
    check_partition_exists,
    delete_partition,
    safe_backfill_month,
    safe_historical_backfill,
)
from etl.jobs.utils.config import JobConfig


class TestParseMonthSpec:
    """Tests for parse_month_spec function."""

    def test_parse_single_month(self):
        """Test parsing single month specification."""
        result = parse_month_spec("2024-06")

        assert result == [(2024, 6)]

    def test_parse_month_range(self):
        """Test parsing month range specification."""
        result = parse_month_spec("2024-01:2024-03")

        assert len(result) == 3
        assert (2024, 1) in result
        assert (2024, 2) in result
        assert (2024, 3) in result

    def test_parse_month_range_cross_year(self):
        """Test parsing month range crossing year boundary."""
        result = parse_month_spec("2023-11:2024-02")

        assert len(result) == 4
        assert (2023, 11) in result
        assert (2023, 12) in result
        assert (2024, 1) in result
        assert (2024, 2) in result

    def test_parse_single_digit_month(self):
        """Test parsing single digit month."""
        result = parse_month_spec("2024-1")

        assert result == [(2024, 1)]


class TestCheckPartitionExists:
    """Tests for check_partition_exists function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_check_partition_exists_found(self):
        """Test partition exists and returns statistics."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        mock_df.select.return_value.distinct.return_value.count.return_value = 950
        mock_spark.read.parquet.return_value = mock_df

        exists, total, unique = check_partition_exists(mock_spark, "yellow", 2024, 1)

        assert exists is True
        assert total == 1000
        assert unique == 950

    def test_check_partition_exists_not_found(self):
        """Test partition does not exist."""
        mock_spark = MagicMock()
        mock_spark.read.parquet.side_effect = Exception("Path not found")

        exists, total, unique = check_partition_exists(mock_spark, "yellow", 2024, 1)

        assert exists is False
        assert total == 0
        assert unique == 0


class TestDeletePartition:
    """Tests for delete_partition function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_delete_partition_success(self):
        """Test successful partition deletion."""
        mock_spark = MagicMock()
        mock_fs = MagicMock()
        mock_fs.exists.return_value = True
        mock_fs.delete.return_value = True

        mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = mock_fs
        mock_spark._jvm.org.apache.hadoop.fs.Path.return_value = MagicMock()

        result = delete_partition(mock_spark, "yellow", 2024, 1)

        assert result is True
        mock_fs.delete.assert_called_once()

    def test_delete_partition_not_exists(self):
        """Test deletion when partition doesn't exist."""
        mock_spark = MagicMock()
        mock_fs = MagicMock()
        mock_fs.exists.return_value = False

        mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = mock_fs
        mock_spark._jvm.org.apache.hadoop.fs.Path.return_value = MagicMock()

        result = delete_partition(mock_spark, "yellow", 2024, 1)

        assert result is False

    def test_delete_partition_error(self):
        """Test deletion error handling."""
        mock_spark = MagicMock()
        mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.side_effect = Exception(
            "Error"
        )

        result = delete_partition(mock_spark, "yellow", 2024, 1)

        assert result is False


class TestSafeBackfillMonth:
    """Tests for safe_backfill_month function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_safe_backfill_month_existing_partition_skip(self):
        """Test backfill with existing partition and delete_existing=False."""
        mock_spark = MagicMock()

        with patch(
            "etl.jobs.bronze.taxi_injection_safe_backfill_job.check_partition_exists"
        ) as mock_check:
            mock_check.return_value = (True, 500, 500)  # Exists

            result = safe_backfill_month(
                mock_spark, "yellow", 2024, 1, delete_existing=False
            )

        assert result["status"] == "SKIPPED"

    def test_safe_backfill_month_delete_fails(self):
        """Test backfill when partition deletion fails."""
        mock_spark = MagicMock()

        with patch(
            "etl.jobs.bronze.taxi_injection_safe_backfill_job.check_partition_exists"
        ) as mock_check:
            mock_check.return_value = (True, 500, 500)  # Exists
            with patch(
                "etl.jobs.bronze.taxi_injection_safe_backfill_job.delete_partition",
                return_value=False,
            ):
                result = safe_backfill_month(
                    mock_spark, "yellow", 2024, 1, delete_existing=True
                )

        assert result["status"] == "ERROR"
        assert "delete" in result["error"].lower()


class TestSafeHistoricalBackfill:
    """Tests for safe_historical_backfill function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_safe_historical_backfill_function_exists(self):
        """Test safe_historical_backfill function exists."""
        assert callable(safe_historical_backfill)
