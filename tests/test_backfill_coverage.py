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
        mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.side_effect = Exception("Error")
        
        result = delete_partition(mock_spark, "yellow", 2024, 1)
        
        assert result is False


class TestSafeBackfillMonth:
    """Tests for safe_backfill_month function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_safe_backfill_month_new_partition(self):
        """Test backfill for new partition (no existing data)."""
        mock_spark = MagicMock()
        
        with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.check_partition_exists") as mock_check:
            mock_check.side_effect = [
                (False, 0, 0),  # Before: doesn't exist
                (True, 1000, 1000),  # After: exists
            ]
            with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.TaxiIngestionJob") as mock_job_class:
                mock_job = MagicMock()
                mock_job.run.return_value = True
                mock_job.get_metrics.return_value = {
                    "extract_duration_seconds": 1.0,
                    "transform_duration_seconds": 0.5,
                    "load_duration_seconds": 2.0,
                    "total_duration_seconds": 3.5,
                }
                mock_job_class.return_value = mock_job
                
                result = safe_backfill_month(mock_spark, "yellow", 2024, 1, delete_existing=True)
        
        assert result["status"] == "SUCCESS"
        assert result["records_after"] == 1000

    def test_safe_backfill_month_existing_partition_delete(self):
        """Test backfill with existing partition and delete_existing=True."""
        mock_spark = MagicMock()
        
        with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.check_partition_exists") as mock_check:
            mock_check.side_effect = [
                (True, 500, 450),  # Before: exists with duplicates
                (True, 1000, 1000),  # After: clean data
            ]
            with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.delete_partition", return_value=True):
                with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.TaxiIngestionJob") as mock_job_class:
                    mock_job = MagicMock()
                    mock_job.run.return_value = True
                    mock_job.get_metrics.return_value = {
                        "extract_duration_seconds": 1.0,
                        "transform_duration_seconds": 0.5,
                        "load_duration_seconds": 2.0,
                        "total_duration_seconds": 3.5,
                    }
                    mock_job_class.return_value = mock_job
                    
                    result = safe_backfill_month(mock_spark, "yellow", 2024, 1, delete_existing=True)
        
        assert result["status"] == "SUCCESS"
        assert result["records_before"] == 500
        assert result["duplicates_before"] == 50

    def test_safe_backfill_month_existing_partition_skip(self):
        """Test backfill with existing partition and delete_existing=False."""
        mock_spark = MagicMock()
        
        with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.check_partition_exists") as mock_check:
            mock_check.return_value = (True, 500, 500)  # Exists
            
            result = safe_backfill_month(mock_spark, "yellow", 2024, 1, delete_existing=False)
        
        assert result["status"] == "SKIPPED"

    def test_safe_backfill_month_delete_fails(self):
        """Test backfill when partition deletion fails."""
        mock_spark = MagicMock()
        
        with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.check_partition_exists") as mock_check:
            mock_check.return_value = (True, 500, 500)  # Exists
            with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.delete_partition", return_value=False):
                result = safe_backfill_month(mock_spark, "yellow", 2024, 1, delete_existing=True)
        
        assert result["status"] == "ERROR"
        assert "delete" in result["error"].lower()

    def test_safe_backfill_month_ingestion_fails(self):
        """Test backfill when ingestion job fails."""
        mock_spark = MagicMock()
        
        with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.check_partition_exists") as mock_check:
            mock_check.return_value = (False, 0, 0)  # Doesn't exist
            with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.TaxiIngestionJob") as mock_job_class:
                mock_job = MagicMock()
                mock_job.run.return_value = False
                mock_job_class.return_value = mock_job
                
                result = safe_backfill_month(mock_spark, "yellow", 2024, 1, delete_existing=True)
        
        assert result["status"] == "FAILED"

    def test_safe_backfill_month_ingestion_exception(self):
        """Test backfill when ingestion job raises exception."""
        mock_spark = MagicMock()
        
        with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.check_partition_exists") as mock_check:
            mock_check.return_value = (False, 0, 0)  # Doesn't exist
            with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.TaxiIngestionJob") as mock_job_class:
                mock_job = MagicMock()
                mock_job.run.side_effect = Exception("Job error")
                mock_job_class.return_value = mock_job
                
                result = safe_backfill_month(mock_spark, "yellow", 2024, 1, delete_existing=True)
        
        assert result["status"] == "ERROR"
        assert "Job error" in result["error"]

    def test_safe_backfill_month_partition_not_found_after(self):
        """Test backfill when partition not found after ingestion."""
        mock_spark = MagicMock()
        
        with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.check_partition_exists") as mock_check:
            mock_check.side_effect = [
                (False, 0, 0),  # Before: doesn't exist
                (False, 0, 0),  # After: still doesn't exist
            ]
            with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.TaxiIngestionJob") as mock_job_class:
                mock_job = MagicMock()
                mock_job.run.return_value = True
                mock_job_class.return_value = mock_job
                
                result = safe_backfill_month(mock_spark, "yellow", 2024, 1, delete_existing=True)
        
        assert result["status"] == "FAILED"
        assert "not found after" in result["error"].lower()


class TestSafeHistoricalBackfill:
    """Tests for safe_historical_backfill function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_safe_historical_backfill_single_month(self):
        """Test historical backfill with single month."""
        with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.SparkSessionManager") as mock_manager:
            mock_spark = MagicMock()
            mock_manager.get_session.return_value = mock_spark
            
            with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.safe_backfill_month") as mock_backfill:
                mock_backfill.return_value = {
                    "period": "2024-01",
                    "status": "SUCCESS",
                    "records_before": 0,
                    "records_after": 1000,
                    "duplicates_before": 0,
                    "duplicates_after": 0,
                    "error": None,
                }
                
                results = safe_historical_backfill("yellow", [(2024, 1)], delete_existing=True)
        
        assert "2024-01" in results
        assert results["2024-01"]["status"] == "SUCCESS"

    def test_safe_historical_backfill_multiple_months(self):
        """Test historical backfill with multiple months."""
        with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.SparkSessionManager") as mock_manager:
            mock_spark = MagicMock()
            mock_manager.get_session.return_value = mock_spark
            
            with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.safe_backfill_month") as mock_backfill:
                mock_backfill.side_effect = [
                    {"period": "2024-01", "status": "SUCCESS", "records_before": 0, "records_after": 1000, "duplicates_before": 0, "duplicates_after": 0, "error": None},
                    {"period": "2024-02", "status": "SUCCESS", "records_before": 0, "records_after": 1100, "duplicates_before": 0, "duplicates_after": 0, "error": None},
                    {"period": "2024-03", "status": "FAILED", "records_before": 0, "records_after": 0, "duplicates_before": 0, "duplicates_after": 0, "error": "Test error"},
                ]
                
                results = safe_historical_backfill("yellow", [(2024, 1), (2024, 2), (2024, 3)], delete_existing=True)
        
        assert len(results) == 3
        assert results["2024-01"]["status"] == "SUCCESS"
        assert results["2024-02"]["status"] == "SUCCESS"
        assert results["2024-03"]["status"] == "FAILED"

    def test_safe_historical_backfill_no_delete(self):
        """Test historical backfill with delete_existing=False."""
        with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.SparkSessionManager") as mock_manager:
            mock_spark = MagicMock()
            mock_manager.get_session.return_value = mock_spark
            
            with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.safe_backfill_month") as mock_backfill:
                mock_backfill.return_value = {
                    "period": "2024-01",
                    "status": "SKIPPED",
                    "records_before": 1000,
                    "records_after": 0,
                    "duplicates_before": 0,
                    "duplicates_after": 0,
                    "error": None,
                }
                
                results = safe_historical_backfill("yellow", [(2024, 1)], delete_existing=False)
        
        assert results["2024-01"]["status"] == "SKIPPED"


class TestSafeBackfillMonthWithDuplicatesAfter:
    """Tests for safe_backfill_month with duplicates after ingestion."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_safe_backfill_month_duplicates_after(self):
        """Test backfill logs warning when duplicates found after ingestion."""
        mock_spark = MagicMock()
        
        with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.check_partition_exists") as mock_check:
            mock_check.side_effect = [
                (False, 0, 0),  # Before: doesn't exist
                (True, 1000, 950),  # After: exists with 50 duplicates
            ]
            with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.TaxiIngestionJob") as mock_job_class:
                mock_job = MagicMock()
                mock_job.run.return_value = True
                mock_job.get_metrics.return_value = {
                    "extract_duration_seconds": 1.0,
                    "transform_duration_seconds": 0.5,
                    "load_duration_seconds": 2.0,
                    "total_duration_seconds": 3.5,
                }
                mock_job_class.return_value = mock_job
                
                result = safe_backfill_month(mock_spark, "yellow", 2024, 1, delete_existing=True)
        
        assert result["status"] == "SUCCESS"
        assert result["duplicates_after"] == 50


class TestSafeBackfillMonthComparison:
    """Tests for safe_backfill_month record comparison."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_safe_backfill_month_record_comparison(self):
        """Test backfill compares records before and after."""
        mock_spark = MagicMock()
        
        with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.check_partition_exists") as mock_check:
            mock_check.side_effect = [
                (True, 500, 450),  # Before: 500 records, 50 duplicates
                (True, 1000, 1000),  # After: 1000 records, no duplicates
            ]
            with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.delete_partition", return_value=True):
                with patch("etl.jobs.bronze.taxi_injection_safe_backfill_job.TaxiIngestionJob") as mock_job_class:
                    mock_job = MagicMock()
                    mock_job.run.return_value = True
                    mock_job.get_metrics.return_value = {
                        "extract_duration_seconds": 1.0,
                        "transform_duration_seconds": 0.5,
                        "load_duration_seconds": 2.0,
                        "total_duration_seconds": 3.5,
                    }
                    mock_job_class.return_value = mock_job
                    
                    result = safe_backfill_month(mock_spark, "yellow", 2024, 1, delete_existing=True)
        
        assert result["status"] == "SUCCESS"
        assert result["records_before"] == 500
        assert result["records_after"] == 1000
        # Change is +500 records
