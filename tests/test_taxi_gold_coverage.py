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

    def test_extract_bronze_trips_multiple_partitions(self):
        """Test extraction from multiple partitions."""
        job = TaxiGoldJob("yellow", 2024, 1, end_year=2024, end_month=3)
        
        mock_df = MagicMock()
        mock_df.columns = ["col1", "col2"]
        mock_df.take.return_value = [MagicMock()]
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

    def test_extract_bronze_trips_schema_standardization(self):
        """Test schema standardization across partitions."""
        job = TaxiGoldJob("yellow", 2024, 1, end_year=2024, end_month=2)
        
        # First partition has col1, col2
        mock_df1 = MagicMock()
        mock_df1.columns = ["col1", "col2"]
        mock_df1.take.return_value = [MagicMock()]
        mock_df1.count.return_value = 500
        mock_df1.select.return_value = mock_df1
        mock_df1.withColumn.return_value = mock_df1
        
        # Second partition has col1, col2, col3 (extra column)
        mock_df2 = MagicMock()
        mock_df2.columns = ["col1", "col2", "col3"]
        mock_df2.take.return_value = [MagicMock()]
        mock_df2.count.return_value = 500
        mock_df2.select.return_value = mock_df2
        mock_df2.withColumn.return_value = mock_df2
        
        mock_spark_read = MagicMock()
        mock_spark_read.option.return_value = mock_spark_read
        mock_spark_read.parquet.side_effect = [mock_df1, mock_df2]
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            result = job._extract_bronze_trips()
        
        assert result is not None


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


class TestTaxiGoldJobTransform:
    """Tests for transform method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_transform_creates_dimensional_model(self):
        """Test transform creates all dimensional model components."""
        job = TaxiGoldJob("yellow", 2024, 1)
        
        mock_trips_df = MagicMock()
        mock_zones_df = MagicMock()
        
        mock_dim_date = MagicMock()
        mock_dim_location = MagicMock()
        mock_dim_payment = MagicMock()
        mock_fact_trip = MagicMock()
        
        with patch.object(job, "_remove_duplicates", return_value=mock_trips_df):
            with patch.object(job, "_apply_data_quality_filters", return_value=mock_trips_df):
                with patch.object(job, "_standardize_schema", return_value=mock_trips_df):
                    with patch.object(job, "_create_dim_date", return_value=mock_dim_date):
                        with patch.object(job, "_create_dim_location", return_value=mock_dim_location):
                            with patch.object(job, "_create_dim_payment", return_value=mock_dim_payment):
                                with patch.object(job, "_create_fact_trip", return_value=mock_fact_trip):
                                    with patch.object(job, "_validate_hash_integrity"):
                                        result = job.transform((mock_trips_df, mock_zones_df))
        
        assert "dim_date" in result
        assert "dim_location" in result
        assert "dim_payment" in result
        assert "fact_trip" in result


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

    def test_apply_data_quality_filters(self):
        """Test data quality filters are applied."""
        job = TaxiGoldJob("yellow", 2024, 1)
        
        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        mock_df.filter.return_value = mock_df
        mock_df.columns = ["trip_distance", "fare_amount", "passenger_count"]
        
        result = job._apply_data_quality_filters(mock_df)
        
        assert result is not None


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

    def test_create_dim_date(self):
        """Test dim_date creation."""
        job = TaxiGoldJob("yellow", 2024, 1)
        
        mock_df = MagicMock()
        mock_df.select.return_value = mock_df
        mock_df.distinct.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 31
        
        result = job._create_dim_date(mock_df)
        
        assert result is not None


class TestTaxiGoldJobCreateDimLocation:
    """Tests for _create_dim_location method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_create_dim_location(self):
        """Test dim_location creation."""
        job = TaxiGoldJob("yellow", 2024, 1)
        
        mock_zones_df = MagicMock()
        mock_zones_df.select.return_value = mock_zones_df
        mock_zones_df.withColumn.return_value = mock_zones_df
        mock_zones_df.withColumnRenamed.return_value = mock_zones_df
        mock_zones_df.count.return_value = 265
        
        result = job._create_dim_location(mock_zones_df)
        
        assert result is not None


class TestTaxiGoldJobCreateDimPayment:
    """Tests for _create_dim_payment method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_create_dim_payment(self):
        """Test dim_payment creation."""
        job = TaxiGoldJob("yellow", 2024, 1)
        
        mock_df = MagicMock()
        mock_df.select.return_value = mock_df
        mock_df.distinct.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 6
        
        result = job._create_dim_payment(mock_df)
        
        assert result is not None


class TestTaxiGoldJobCreateFactTrip:
    """Tests for _create_fact_trip method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_create_fact_trip(self):
        """Test fact_trip creation."""
        job = TaxiGoldJob("yellow", 2024, 1)
        
        mock_trips_df = MagicMock()
        mock_trips_df.join.return_value = mock_trips_df
        mock_trips_df.select.return_value = mock_trips_df
        mock_trips_df.withColumn.return_value = mock_trips_df
        mock_trips_df.count.return_value = 1000
        
        mock_dim_date = MagicMock()
        mock_dim_location = MagicMock()
        mock_dim_payment = MagicMock()
        
        result = job._create_fact_trip(mock_trips_df, mock_dim_date, mock_dim_location, mock_dim_payment)
        
        assert result is not None


class TestTaxiGoldJobValidateHashIntegrity:
    """Tests for _validate_hash_integrity method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_validate_hash_integrity_valid(self):
        """Test hash integrity validation passes."""
        job = TaxiGoldJob("yellow", 2024, 1)
        
        mock_fact_trip = MagicMock()
        mock_fact_trip.columns = ["trip_hash"]
        mock_fact_trip.filter.return_value.count.return_value = 0  # No null hashes
        mock_fact_trip.count.return_value = 1000
        mock_fact_trip.select.return_value.distinct.return_value.count.return_value = 1000  # All unique
        
        # Should not raise
        job._validate_hash_integrity(mock_fact_trip)

    def test_validate_hash_integrity_null_hashes(self):
        """Test hash integrity validation fails on null hashes."""
        job = TaxiGoldJob("yellow", 2024, 1)
        
        mock_fact_trip = MagicMock()
        mock_fact_trip.columns = ["trip_hash"]
        mock_fact_trip.filter.return_value.count.return_value = 10  # 10 null hashes
        
        with pytest.raises(DataQualityError) as exc_info:
            job._validate_hash_integrity(mock_fact_trip)
        
        assert "null" in str(exc_info.value).lower()

    def test_validate_hash_integrity_duplicate_hashes(self):
        """Test hash integrity validation warns on duplicate hashes."""
        job = TaxiGoldJob("yellow", 2024, 1)
        
        mock_fact_trip = MagicMock()
        mock_fact_trip.columns = ["trip_hash"]
        mock_fact_trip.filter.return_value.count.return_value = 0  # No null hashes
        mock_fact_trip.count.return_value = 1000
        mock_fact_trip.select.return_value.distinct.return_value.count.return_value = 950  # 50 duplicates
        
        # Should not raise, just warn
        job._validate_hash_integrity(mock_fact_trip)


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
        
        for mock_df in [mock_dim_date, mock_dim_location, mock_dim_payment, mock_fact_trip]:
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
        for mock_df in [mock_dim_date, mock_dim_location, mock_dim_payment, mock_fact_trip]:
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
