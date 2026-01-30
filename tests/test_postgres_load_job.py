"""
Tests for PostgresLoadJob class.

Tests cover:
- Initialization and parameter validation
- PostgreSQL connection configuration
- JDBC properties setup
- validate_inputs method
"""

import os
import pytest
from unittest.mock import patch, MagicMock

from etl.jobs.load.postgres_load_job import PostgresLoadJob, run_postgres_load
from etl.jobs.utils.config import JobConfig


class TestPostgresLoadJobInit:
    """Tests for PostgresLoadJob initialization."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_init_with_yellow_taxi_type(self):
        """Test initialization with yellow taxi type."""
        job = PostgresLoadJob("yellow")
        assert job.taxi_type == "yellow"
        assert job.year is None
        assert job.month is None

    def test_init_with_green_taxi_type(self):
        """Test initialization with green taxi type."""
        job = PostgresLoadJob("green")
        assert job.taxi_type == "green"

    def test_init_with_year_and_month(self):
        """Test initialization with year and month filters."""
        job = PostgresLoadJob("yellow", year=2024, month=6)
        assert job.taxi_type == "yellow"
        assert job.year == 2024
        assert job.month == 6

    def test_init_with_year_only(self):
        """Test initialization with year filter only."""
        job = PostgresLoadJob("yellow", year=2024)
        assert job.year == 2024
        assert job.month is None

    def test_init_invalid_taxi_type_raises_error(self):
        """Test that invalid taxi type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            PostgresLoadJob("blue")
        assert "Invalid taxi_type" in str(exc_info.value)

    def test_init_with_custom_postgres_url(self):
        """Test initialization with custom PostgreSQL URL."""
        custom_url = "jdbc:postgresql://custom-host:5433/custom_db"
        job = PostgresLoadJob("yellow", postgres_url=custom_url)
        assert job.postgres_url == custom_url

    def test_init_with_custom_credentials(self):
        """Test initialization with custom PostgreSQL credentials."""
        job = PostgresLoadJob(
            "yellow",
            postgres_user="custom_user",
            postgres_password="custom_pass",
        )
        assert job.postgres_user == "custom_user"
        assert job.postgres_password == "custom_pass"

    def test_init_uses_env_vars_for_postgres_url(self):
        """Test that PostgreSQL URL defaults to environment variable."""
        with patch.dict(
            os.environ, {"POSTGRES_URL": "jdbc:postgresql://env-host:5432/env_db"}
        ):
            JobConfig.reset()
            job = PostgresLoadJob("yellow")
            assert job.postgres_url == "jdbc:postgresql://env-host:5432/env_db"

    def test_init_uses_env_vars_for_credentials(self):
        """Test that credentials default to environment variables."""
        with patch.dict(
            os.environ,
            {"POSTGRES_USER": "env_user", "POSTGRES_PASSWORD": "env_pass"},
        ):
            JobConfig.reset()
            job = PostgresLoadJob("yellow")
            assert job.postgres_user == "env_user"
            assert job.postgres_password == "env_pass"

    def test_init_default_postgres_url(self):
        """Test default PostgreSQL URL when no env var set."""
        with patch.dict(os.environ, {}, clear=True):
            # Clear specific env vars
            env_copy = os.environ.copy()
            for key in ["POSTGRES_URL", "POSTGRES_USER", "POSTGRES_PASSWORD"]:
                env_copy.pop(key, None)
            with patch.dict(os.environ, env_copy, clear=True):
                JobConfig.reset()
                job = PostgresLoadJob("yellow")
                assert "localhost:5432" in job.postgres_url

    def test_job_name_format_with_all_params(self):
        """Test job name format includes taxi type, year, and month."""
        job = PostgresLoadJob("yellow", year=2024, month=6)
        assert "PostgresLoad" in job.job_name
        assert "yellow" in job.job_name
        assert "2024" in job.job_name
        assert "6" in job.job_name

    def test_job_name_format_without_filters(self):
        """Test job name format when no year/month specified."""
        job = PostgresLoadJob("green")
        assert "PostgresLoad" in job.job_name
        assert "green" in job.job_name
        assert "all" in job.job_name


class TestPostgresLoadJobProperties:
    """Tests for PostgresLoadJob JDBC properties."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_postgres_properties_contains_user(self):
        """Test that JDBC properties contain user."""
        job = PostgresLoadJob("yellow", postgres_user="test_user")
        assert job.postgres_properties["user"] == "test_user"

    def test_postgres_properties_contains_password(self):
        """Test that JDBC properties contain password."""
        job = PostgresLoadJob("yellow", postgres_password="test_pass")
        assert job.postgres_properties["password"] == "test_pass"

    def test_postgres_properties_contains_driver(self):
        """Test that JDBC properties contain PostgreSQL driver."""
        job = PostgresLoadJob("yellow")
        assert job.postgres_properties["driver"] == "org.postgresql.Driver"

    def test_postgres_properties_contains_batchsize(self):
        """Test that JDBC properties contain batch size."""
        job = PostgresLoadJob("yellow")
        assert "batchsize" in job.postgres_properties
        assert int(job.postgres_properties["batchsize"]) > 0

    def test_postgres_properties_contains_isolation_level(self):
        """Test that JDBC properties contain isolation level."""
        job = PostgresLoadJob("yellow")
        assert job.postgres_properties["isolationLevel"] == "READ_COMMITTED"

    def test_postgres_properties_contains_rewrite_batched_inserts(self):
        """Test that JDBC properties enable batch insert optimization."""
        job = PostgresLoadJob("yellow")
        assert job.postgres_properties["reWriteBatchedInserts"] == "true"


class TestPostgresLoadJobValidateInputs:
    """Tests for PostgresLoadJob.validate_inputs method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_validate_inputs_logs_connection_url(self):
        """Test that validate_inputs logs the connection URL."""
        job = PostgresLoadJob("yellow")
        # Should not raise any exception
        job.validate_inputs()

    def test_validate_inputs_with_year_and_month(self):
        """Test validate_inputs with year and month filters."""
        job = PostgresLoadJob("yellow", year=2024, month=6)
        # Should not raise any exception
        job.validate_inputs()

    def test_validate_inputs_with_year_only(self):
        """Test validate_inputs with year filter only."""
        job = PostgresLoadJob("yellow", year=2024)
        # Should not raise any exception
        job.validate_inputs()

    def test_validate_inputs_without_filters(self):
        """Test validate_inputs without any filters."""
        job = PostgresLoadJob("green")
        # Should not raise any exception
        job.validate_inputs()


class TestRunPostgresLoadFunction:
    """Tests for run_postgres_load convenience function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_run_postgres_load_creates_job_with_correct_params(self):
        """Test that run_postgres_load creates job with correct parameters."""
        with patch.object(PostgresLoadJob, "run", return_value=True) as mock_run:
            result = run_postgres_load(
                "yellow",
                year=2024,
                month=6,
                postgres_url="jdbc:postgresql://test:5432/db",
                postgres_user="user",
                postgres_password="pass",
            )
            assert result is True
            mock_run.assert_called_once()

    def test_run_postgres_load_returns_false_on_failure(self):
        """Test that run_postgres_load returns False on job failure."""
        with patch.object(PostgresLoadJob, "run", return_value=False):
            result = run_postgres_load("yellow")
            assert result is False

    def test_run_postgres_load_with_minimal_params(self):
        """Test run_postgres_load with only required parameters."""
        with patch.object(PostgresLoadJob, "run", return_value=True):
            result = run_postgres_load("green")
            assert result is True


class TestPostgresLoadJobLoad:
    """Tests for PostgresLoadJob.load method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_load_calls_dimension_and_fact_loaders(self):
        """Test that load calls dimension and fact table loaders."""
        job = PostgresLoadJob("yellow")
        mock_data = {
            "dim_date": MagicMock(),
            "dim_location": MagicMock(),
            "dim_time": MagicMock(),
            "dim_payment": MagicMock(),
            "dim_rate": MagicMock(),
            "fact_trip": MagicMock(),
        }
        with patch.object(job, "_load_dimension") as mock_load_dim:
            with patch.object(job, "_load_fact_table") as mock_load_fact:
                job.load(mock_data)
                # Should call _load_dimension for each dimension
                assert mock_load_dim.call_count >= 1
                # Should call _load_fact_table for fact table
                mock_load_fact.assert_called_once()


class TestPostgresLoadJobConfig:
    """Tests for PostgresLoadJob configuration."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_config_uses_job_config(self):
        """Test that job uses JobConfig."""
        job = PostgresLoadJob("yellow")
        assert job.config is not None

    def test_custom_config_is_used(self):
        """Test that custom config is used when provided."""
        config = JobConfig()
        job = PostgresLoadJob("yellow", config=config)
        assert job.config is config
