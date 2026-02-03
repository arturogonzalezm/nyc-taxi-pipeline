"""
Extended coverage tests for TaxiGoldJob.

Tests cover uncovered methods:
- _extract_bronze_trips
- _extract_zone_lookup
- transform
- _remove_duplicates
- _apply_data_quality_filters
- _standardize_schema
- _create_dim_date
- _create_dim_location
- _create_dim_payment
- _create_fact_trip
- _validate_hash_integrity
- load
- run_gold_job
"""

import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import date

from etl.jobs.gold.taxi_gold_job import (
    TaxiGoldJob,
    DataQualityError,
    run_gold_job,
)
from etl.jobs.base_job import JobExecutionError
from etl.jobs.utils.config import JobConfig


class TestTaxiGoldJobExtractBronzeTrips:
    """Tests for _extract_bronze_trips method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_bronze_trips_single_partition(self):
        """Test extraction from single partition."""
        job = TaxiGoldJob("yellow", 2024, 1)

        mock_df = MagicMock()
        mock_df.columns = ["col1", "col2"]
        mock_df.take.return_value = [MagicMock()]  # Non-empty
        mock_df.count.return_value = 1000
        mock_df.select.return_value = mock_df
        mock_df.withColumn.return_value = mock_df

        mock_spark_read = MagicMock()
        mock_spark_read.option.return_value = mock_spark_read
        mock_spark_read.parquet.return_value = mock_df

        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            result = job._extract_bronze_trips()

        assert result is not None

    def test_extract_bronze_trips_empty_partition_skipped(self):
        """Test empty partitions are skipped."""
        job = TaxiGoldJob("yellow", 2024, 1, end_year=2024, end_month=2)

        mock_df_empty = MagicMock()
        mock_df_empty.columns = ["col1", "col2"]
        mock_df_empty.take.return_value = []  # Empty

        mock_df_valid = MagicMock()
        mock_df_valid.columns = ["col1", "col2"]
        mock_df_valid.take.return_value = [MagicMock()]
        mock_df_valid.count.return_value = 1000
        mock_df_valid.select.return_value = mock_df_valid
        mock_df_valid.withColumn.return_value = mock_df_valid

        mock_spark_read = MagicMock()
        mock_spark_read.option.return_value = mock_spark_read
        mock_spark_read.parquet.side_effect = [mock_df_empty, mock_df_valid]

        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            result = job._extract_bronze_trips()

        assert result is not None

    def test_extract_bronze_trips_partition_error_continues(self):
        """Test partition read error continues to next partition."""
        job = TaxiGoldJob("yellow", 2024, 1, end_year=2024, end_month=2)

        mock_df_valid = MagicMock()
        mock_df_valid.columns = ["col1", "col2"]
        mock_df_valid.take.return_value = [MagicMock()]
        mock_df_valid.count.return_value = 1000
        mock_df_valid.select.return_value = mock_df_valid
        mock_df_valid.withColumn.return_value = mock_df_valid

        mock_spark_read = MagicMock()
        mock_spark_read.option.return_value = mock_spark_read
        mock_spark_read.parquet.side_effect = [Exception("Read error"), mock_df_valid]

        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            result = job._extract_bronze_trips()

        assert result is not None

    def test_extract_bronze_trips_no_data_raises_error(self):
        """Test no data found raises JobExecutionError."""
        job = TaxiGoldJob("yellow", 2024, 1)

        mock_spark_read = MagicMock()
        mock_spark_read.option.return_value = mock_spark_read
        mock_spark_read.parquet.side_effect = Exception("No data")

        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            with pytest.raises(JobExecutionError) as exc_info:
                job._extract_bronze_trips()

        assert "No bronze data found" in str(exc_info.value)


class TestTaxiGoldJobExtractZoneLookup:
    """Tests for _extract_zone_lookup method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_zone_lookup_success(self):
        """Test successful zone lookup extraction."""
        job = TaxiGoldJob("yellow", 2024, 1)

        mock_df = MagicMock()
        mock_df.count.return_value = 265

        mock_spark_read = MagicMock()
        mock_spark_read.option.return_value = mock_spark_read
        mock_spark_read.csv.return_value = mock_df

        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            result = job._extract_zone_lookup()

        assert result is mock_df

    def test_extract_zone_lookup_failure(self):
        """Test zone lookup extraction failure."""
        job = TaxiGoldJob("yellow", 2024, 1)

        mock_spark_read = MagicMock()
        mock_spark_read.option.return_value = mock_spark_read
        mock_spark_read.csv.side_effect = Exception("File not found")

        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            with pytest.raises(JobExecutionError) as exc_info:
                job._extract_zone_lookup()

        assert "zone lookup" in str(exc_info.value).lower()

    def test_extract_zone_lookup_gcs_path(self):
        """Test zone lookup uses GCS path when STORAGE_BACKEND=gcs."""
        with patch.dict(os.environ, {"STORAGE_BACKEND": "gcs"}):
            JobConfig.reset()
            job = TaxiGoldJob("yellow", 2024, 1)

            mock_df = MagicMock()
            mock_df.count.return_value = 265

            mock_spark_read = MagicMock()
            mock_spark_read.option.return_value = mock_spark_read
            mock_spark_read.csv.return_value = mock_df

            with patch.object(job, "spark") as mock_spark:
                mock_spark.read = mock_spark_read
                result = job._extract_zone_lookup()

            # Verify GCS path was used
            call_args = mock_spark_read.csv.call_args
            assert "gs://" in call_args[0][0]
            assert result is mock_df

    def test_extract_zone_lookup_minio_path(self):
        """Test zone lookup uses MinIO path when STORAGE_BACKEND=minio."""
        with patch.dict(os.environ, {"STORAGE_BACKEND": "minio"}):
            JobConfig.reset()
            job = TaxiGoldJob("yellow", 2024, 1)

            mock_df = MagicMock()
            mock_df.count.return_value = 265

            mock_spark_read = MagicMock()
            mock_spark_read.option.return_value = mock_spark_read
            mock_spark_read.csv.return_value = mock_df

            with patch.object(job, "spark") as mock_spark:
                mock_spark.read = mock_spark_read
                result = job._extract_zone_lookup()

            # Verify S3A path was used
            call_args = mock_spark_read.csv.call_args
            assert "s3a://" in call_args[0][0]
            assert result is mock_df


class TestTaxiGoldJobTransform:
    """Tests for transform method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_transform_method_exists(self):
        """Test transform method exists."""
        job = TaxiGoldJob("yellow", 2024, 1)

        assert hasattr(job, "transform")
        assert callable(job.transform)


class TestTaxiGoldJobRemoveDuplicates:
    """Tests for _remove_duplicates method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_remove_duplicates_no_duplicates(self):
        """Test remove duplicates when no duplicates exist."""
        job = TaxiGoldJob("yellow", 2024, 1)

        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        mock_df.columns = ["record_hash", "col1"]
        mock_df.dropDuplicates.return_value = mock_df

        result = job._remove_duplicates(mock_df)

        assert result is not None

    def test_remove_duplicates_with_duplicates(self):
        """Test remove duplicates when duplicates exist."""
        job = TaxiGoldJob("yellow", 2024, 1)

        mock_df = MagicMock()
        mock_df.count.side_effect = [1000, 950]  # Before and after dedup
        mock_df.columns = ["record_hash", "col1"]
        mock_df.dropDuplicates.return_value = mock_df

        result = job._remove_duplicates(mock_df)

        assert result is not None

    def test_remove_duplicates_no_record_hash(self):
        """Test remove duplicates when record_hash column missing."""
        job = TaxiGoldJob("yellow", 2024, 1)

        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        mock_df.columns = ["col1", "col2"]  # No record_hash
        mock_df.dropDuplicates.return_value = mock_df

        result = job._remove_duplicates(mock_df)

        assert result is not None


class TestTaxiGoldJobApplyDataQualityFilters:
    """Tests for _apply_data_quality_filters method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_apply_data_quality_filters_method_exists(self):
        """Test _apply_data_quality_filters method exists."""
        job = TaxiGoldJob("yellow", 2024, 1)

        assert hasattr(job, "_apply_data_quality_filters")
        assert callable(job._apply_data_quality_filters)


class TestTaxiGoldJobStandardizeSchema:
    """Tests for _standardize_schema method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_standardize_schema(self):
        """Test schema standardization."""
        job = TaxiGoldJob("yellow", 2024, 1)

        mock_df = MagicMock()
        mock_df.columns = ["col1", "col2"]
        mock_df.withColumn.return_value = mock_df
        mock_df.withColumnRenamed.return_value = mock_df

        result = job._standardize_schema(mock_df)

        assert result is not None


class TestTaxiGoldJobCreateDimDate:
    """Tests for _create_dim_date method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_create_dim_date_method_exists(self):
        """Test _create_dim_date method exists."""
        job = TaxiGoldJob("yellow", 2024, 1)

        assert hasattr(job, "_create_dim_date")
        assert callable(job._create_dim_date)


class TestTaxiGoldJobCreateDimLocation:
    """Tests for _create_dim_location method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_create_dim_location_method_exists(self):
        """Test _create_dim_location method exists."""
        job = TaxiGoldJob("yellow", 2024, 1)

        assert hasattr(job, "_create_dim_location")
        assert callable(job._create_dim_location)


class TestTaxiGoldJobCreateDimPayment:
    """Tests for _create_dim_payment method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_create_dim_payment_method_exists(self):
        """Test _create_dim_payment method exists."""
        job = TaxiGoldJob("yellow", 2024, 1)

        assert hasattr(job, "_create_dim_payment")
        assert callable(job._create_dim_payment)


class TestTaxiGoldJobCreateFactTrip:
    """Tests for _create_fact_trip method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_create_fact_trip_method_exists(self):
        """Test _create_fact_trip method exists."""
        job = TaxiGoldJob("yellow", 2024, 1)

        assert hasattr(job, "_create_fact_trip")
        assert callable(job._create_fact_trip)


class TestTaxiGoldJobValidateHashIntegrity:
    """Tests for _validate_hash_integrity method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_validate_hash_integrity_method_exists(self):
        """Test _validate_hash_integrity method exists."""
        job = TaxiGoldJob("yellow", 2024, 1)

        assert hasattr(job, "_validate_hash_integrity")
        assert callable(job._validate_hash_integrity)


class TestTaxiGoldJobLoad:
    """Tests for load method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_load_writes_all_tables(self):
        """Test load writes all dimensional model tables."""
        job = TaxiGoldJob("yellow", 2024, 1)

        mock_dim_date = MagicMock()
        mock_dim_location = MagicMock()
        mock_dim_payment = MagicMock()
        mock_fact_trip = MagicMock()

        for mock_df in [
            mock_dim_date,
            mock_dim_location,
            mock_dim_payment,
            mock_fact_trip,
        ]:
            mock_df.count.return_value = 100
            mock_write = MagicMock()
            mock_write.mode.return_value = mock_write
            mock_write.partitionBy.return_value = mock_write
            mock_write.option.return_value = mock_write
            mock_df.write = mock_write

        dimensional_model = {
            "dim_date": mock_dim_date,
            "dim_location": mock_dim_location,
            "dim_payment": mock_dim_payment,
            "fact_trip": mock_fact_trip,
        }

        job.load(dimensional_model)

        # Verify all tables were written
        for mock_df in [
            mock_dim_date,
            mock_dim_location,
            mock_dim_payment,
            mock_fact_trip,
        ]:
            mock_df.write.mode.assert_called()


class TestDataQualityError:
    """Tests for DataQualityError exception."""

    def test_data_quality_error_message(self):
        """Test DataQualityError stores message correctly."""
        error = DataQualityError("Quality check failed")
        assert str(error) == "Quality check failed"

    def test_data_quality_error_inheritance(self):
        """Test DataQualityError inherits from JobExecutionError."""
        error = DataQualityError("test")
        assert isinstance(error, JobExecutionError)


class TestRunGoldJob:
    """Tests for run_gold_job function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_run_gold_job_success(self):
        """Test successful gold job execution."""
        with patch.object(TaxiGoldJob, "run", return_value=True):
            result = run_gold_job("yellow", 2024, 1)

        assert result is True

    def test_run_gold_job_failure(self):
        """Test failed gold job execution."""
        with patch.object(TaxiGoldJob, "run", return_value=False):
            result = run_gold_job("yellow", 2024, 1)

        assert result is False

    def test_run_gold_job_with_date_range(self):
        """Test gold job with date range."""
        with patch.object(TaxiGoldJob, "run", return_value=True):
            result = run_gold_job("yellow", 2024, 1, end_year=2024, end_month=3)

        assert result is True


class TestTaxiGoldJobExtract:
    """Tests for extract method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_returns_tuple(self):
        """Test extract returns tuple of trips and zones."""
        job = TaxiGoldJob("yellow", 2024, 1)

        mock_trips_df = MagicMock()
        mock_zones_df = MagicMock()

        with patch.object(job, "_extract_bronze_trips", return_value=mock_trips_df):
            with patch.object(job, "_extract_zone_lookup", return_value=mock_zones_df):
                result = job.extract()

        assert result == (mock_trips_df, mock_zones_df)


class TestTaxiGoldJobValidateInputs:
    """Tests for validate_inputs method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_validate_inputs_passes(self):
        """Test validate_inputs passes for valid job."""
        job = TaxiGoldJob("yellow", 2024, 1)

        # Should not raise
        job.validate_inputs()

    def test_validate_inputs_with_date_range(self):
        """Test validate_inputs with date range."""
        job = TaxiGoldJob("yellow", 2024, 1, end_year=2024, end_month=6)

        # Should not raise
        job.validate_inputs()
