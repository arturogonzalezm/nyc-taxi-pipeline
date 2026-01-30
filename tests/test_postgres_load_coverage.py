"""
Extended coverage tests for PostgresLoadJob.

Tests cover uncovered methods:
- extract
- transform
- load
- _load_dimension
- _load_fact_table
- _upsert_via_temp_table
- run_postgres_load
"""

import pytest
from unittest.mock import patch, MagicMock

from etl.jobs.load.postgres_load_job import (
    PostgresLoadJob,
    run_postgres_load,
)
from etl.jobs.base_job import JobExecutionError
from etl.jobs.utils.config import JobConfig


class TestPostgresLoadJobExtract:
    """Tests for extract method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_reads_all_dimensions(self):
        """Test extract reads all dimension tables."""
        job = PostgresLoadJob("yellow")
        
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        
        mock_spark_read = MagicMock()
        mock_spark_read.parquet.return_value = mock_df
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            result = job.extract()
        
        assert "dim_date" in result
        assert "dim_location" in result
        assert "dim_payment" in result
        assert "fact_trip" in result

    def test_extract_with_year_filter(self):
        """Test extract applies year filter to fact table."""
        job = PostgresLoadJob("yellow", year=2024)
        
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.filter.return_value = mock_df
        
        mock_spark_read = MagicMock()
        mock_spark_read.parquet.return_value = mock_df
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            result = job.extract()
        
        assert "fact_trip" in result
        mock_df.filter.assert_called()

    def test_extract_with_year_and_month_filter(self):
        """Test extract applies year and month filter to fact table."""
        job = PostgresLoadJob("yellow", year=2024, month=6)
        
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.filter.return_value = mock_df
        
        mock_spark_read = MagicMock()
        mock_spark_read.parquet.return_value = mock_df
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            result = job.extract()
        
        assert "fact_trip" in result

    def test_extract_dimension_read_failure(self):
        """Test extract raises error on dimension read failure."""
        job = PostgresLoadJob("yellow")
        
        mock_spark_read = MagicMock()
        mock_spark_read.parquet.side_effect = Exception("Read error")
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            with pytest.raises(JobExecutionError) as exc_info:
                job.extract()
        
        assert "Failed to read" in str(exc_info.value)

    def test_extract_fact_read_failure(self):
        """Test extract raises error on fact table read failure."""
        job = PostgresLoadJob("yellow")
        
        mock_dim_df = MagicMock()
        mock_dim_df.count.return_value = 100
        
        mock_spark_read = MagicMock()
        # First 3 calls succeed (dimensions), 4th fails (fact)
        mock_spark_read.parquet.side_effect = [
            mock_dim_df, mock_dim_df, mock_dim_df,
            Exception("Fact read error")
        ]
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            with pytest.raises(JobExecutionError) as exc_info:
                job.extract()
        
        assert "Failed to read fact_trip" in str(exc_info.value)


class TestPostgresLoadJobTransform:
    """Tests for transform method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_transform_adds_metadata(self):
        """Test transform adds load metadata to all tables."""
        job = PostgresLoadJob("yellow")
        
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 100
        
        dimensional_model = {
            "dim_date": mock_df,
            "dim_location": mock_df,
            "dim_payment": mock_df,
            "fact_trip": mock_df,
        }
        
        result = job.transform(dimensional_model)
        
        assert len(result) == 4
        # Verify withColumn was called for metadata
        assert mock_df.withColumn.called


class TestPostgresLoadJobLoad:
    """Tests for load method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_load_calls_dimension_and_fact_loaders(self):
        """Test load calls dimension and fact table loaders."""
        job = PostgresLoadJob("yellow")
        
        mock_df = MagicMock()
        
        dimensional_model = {
            "dim_date": mock_df,
            "dim_location": mock_df,
            "dim_payment": mock_df,
            "fact_trip": mock_df,
        }
        
        with patch.object(job, "_load_dimension") as mock_load_dim:
            with patch.object(job, "_load_fact_table") as mock_load_fact:
                job.load(dimensional_model)
        
        # 3 dimension tables
        assert mock_load_dim.call_count == 3
        # 1 fact table
        mock_load_fact.assert_called_once()


class TestPostgresLoadJobLoadDimension:
    """Tests for _load_dimension method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_load_dimension_success(self):
        """Test successful dimension load."""
        job = PostgresLoadJob("yellow")
        
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        
        with patch.object(job, "_upsert_via_temp_table") as mock_upsert:
            job._load_dimension(mock_df, "taxi.dim_date", "dim_date")
        
        mock_upsert.assert_called_once()

    def test_load_dimension_empty_dataframe(self):
        """Test dimension load with empty dataframe."""
        job = PostgresLoadJob("yellow")
        
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        
        with patch.object(job, "_upsert_via_temp_table") as mock_upsert:
            job._load_dimension(mock_df, "taxi.dim_date", "dim_date")
        
        # Should still call upsert even with empty df
        mock_upsert.assert_called_once()


class TestPostgresLoadJobLoadFactTable:
    """Tests for _load_fact_table method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_load_fact_table_success(self):
        """Test successful fact table load."""
        job = PostgresLoadJob("yellow")
        
        mock_df = MagicMock()
        mock_df.count.return_value = 1000
        
        with patch.object(job, "_upsert_via_temp_table") as mock_upsert:
            job._load_fact_table(mock_df)
        
        mock_upsert.assert_called_once()


class TestPostgresLoadJobUpsertViaTempTable:
    """Tests for _upsert_via_temp_table method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_upsert_via_temp_table_success(self):
        """Test successful upsert via temp table."""
        job = PostgresLoadJob("yellow")
        
        mock_df = MagicMock()
        mock_df.columns = ["id", "name", "value"]
        mock_df.count.return_value = 100
        
        mock_write = MagicMock()
        mock_write.format.return_value = mock_write
        mock_write.option.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_df.write = mock_write
        
        # Mock JDBC connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = MagicMock(return_value=False)
        
        with patch("psycopg2.connect", return_value=mock_connection):
            job._upsert_via_temp_table(mock_df, "taxi.dim_test")
        
        mock_write.save.assert_called()

    def test_upsert_via_temp_table_jdbc_error(self):
        """Test upsert handles JDBC write error."""
        job = PostgresLoadJob("yellow")
        
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.count.return_value = 100
        
        mock_write = MagicMock()
        mock_write.format.return_value = mock_write
        mock_write.option.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.save.side_effect = Exception("JDBC error")
        mock_df.write = mock_write
        
        with pytest.raises(Exception):
            job._upsert_via_temp_table(mock_df, "taxi.dim_test")


class TestRunPostgresLoad:
    """Tests for run_postgres_load function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_run_postgres_load_success(self):
        """Test successful postgres load execution."""
        with patch.object(PostgresLoadJob, "run", return_value=True):
            result = run_postgres_load("yellow")
        
        assert result is True

    def test_run_postgres_load_failure(self):
        """Test failed postgres load execution."""
        with patch.object(PostgresLoadJob, "run", return_value=False):
            result = run_postgres_load("yellow")
        
        assert result is False

    def test_run_postgres_load_with_filters(self):
        """Test postgres load with year and month filters."""
        with patch.object(PostgresLoadJob, "run", return_value=True):
            result = run_postgres_load("yellow", year=2024, month=6)
        
        assert result is True

    def test_run_postgres_load_with_custom_credentials(self):
        """Test postgres load with custom credentials."""
        with patch.object(PostgresLoadJob, "run", return_value=True):
            result = run_postgres_load(
                "yellow",
                postgres_url="jdbc:postgresql://custom:5432/db",
                postgres_user="user",
                postgres_password="pass"
            )
        
        assert result is True


class TestPostgresLoadJobValidateInputs:
    """Tests for validate_inputs method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_validate_inputs_no_filters(self):
        """Test validate_inputs with no filters."""
        job = PostgresLoadJob("yellow")
        
        # Should not raise
        job.validate_inputs()

    def test_validate_inputs_with_year(self):
        """Test validate_inputs with year filter."""
        job = PostgresLoadJob("yellow", year=2024)
        
        # Should not raise
        job.validate_inputs()

    def test_validate_inputs_with_year_and_month(self):
        """Test validate_inputs with year and month filters."""
        job = PostgresLoadJob("yellow", year=2024, month=6)
        
        # Should not raise
        job.validate_inputs()


class TestPostgresLoadJobJdbcProperties:
    """Tests for JDBC properties setup."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_jdbc_properties_set(self):
        """Test JDBC properties are set correctly."""
        job = PostgresLoadJob("yellow")
        
        assert job.jdbc_properties is not None
        assert "user" in job.jdbc_properties
        assert "password" in job.jdbc_properties
        assert "driver" in job.jdbc_properties

    def test_jdbc_properties_custom_credentials(self):
        """Test JDBC properties with custom credentials."""
        job = PostgresLoadJob(
            "yellow",
            postgres_user="custom_user",
            postgres_password="custom_pass"
        )
        
        assert job.jdbc_properties["user"] == "custom_user"
        assert job.jdbc_properties["password"] == "custom_pass"


class TestPostgresLoadJobInit:
    """Tests for PostgresLoadJob initialization."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_init_yellow_taxi(self):
        """Test initialization with yellow taxi."""
        job = PostgresLoadJob("yellow")
        
        assert job.taxi_type == "yellow"

    def test_init_green_taxi(self):
        """Test initialization with green taxi."""
        job = PostgresLoadJob("green")
        
        assert job.taxi_type == "green"

    def test_init_invalid_taxi_type(self):
        """Test initialization with invalid taxi type."""
        with pytest.raises(ValueError) as exc_info:
            PostgresLoadJob("blue")
        
        assert "Invalid taxi_type" in str(exc_info.value)

    def test_init_job_name_format(self):
        """Test job name format."""
        job = PostgresLoadJob("yellow", year=2024, month=6)
        
        assert "PostgresLoad" in job.job_name
        assert "yellow" in job.job_name
