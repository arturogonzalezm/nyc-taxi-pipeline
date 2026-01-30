"""
Tests for ZoneLookupIngestionJob to improve coverage.
"""

import pytest
from unittest.mock import patch

from etl.jobs.bronze.zone_lookup_ingestion_job import (
    ZoneLookupIngestionJob,
    ReferenceDataError,
)
from etl.jobs.base_job import JobExecutionError
from etl.jobs.utils.config import JobConfig


class TestZoneLookupIngestionJobInit:
    """Tests for ZoneLookupIngestionJob initialization."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_init_creates_job(self):
        """Test that job can be initialized."""
        job = ZoneLookupIngestionJob()
        assert job is not None

    def test_init_sets_file_name(self):
        """Test that file_name is set from constant."""
        job = ZoneLookupIngestionJob()
        assert job.file_name == ZoneLookupIngestionJob.FILE_NAME

    def test_init_sets_source_url(self):
        """Test that source_url is set from constant."""
        job = ZoneLookupIngestionJob()
        assert job.source_url == ZoneLookupIngestionJob.SOURCE_URL

    def test_init_job_name(self):
        """Test that job name is set correctly."""
        job = ZoneLookupIngestionJob()
        assert job.job_name == "ZoneLookupIngestion"

    def test_init_with_custom_config(self):
        """Test initialization with custom config."""
        config = JobConfig()
        job = ZoneLookupIngestionJob(config=config)
        assert job.config is config


class TestZoneLookupIngestionJobValidateInputs:
    """Tests for ZoneLookupIngestionJob.validate_inputs method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_validate_inputs_does_not_raise(self):
        """Test that validate_inputs does not raise for valid job."""
        job = ZoneLookupIngestionJob()
        # Should not raise
        job.validate_inputs()


class TestZoneLookupIngestionJobConstants:
    """Tests for ZoneLookupIngestionJob class constants."""

    def test_file_name_constant(self):
        """Test FILE_NAME constant is set correctly."""
        assert ZoneLookupIngestionJob.FILE_NAME == "taxi_zone_lookup.csv"

    def test_source_url_constant(self):
        """Test SOURCE_URL constant is set correctly."""
        assert "taxi_zone_lookup.csv" in ZoneLookupIngestionJob.SOURCE_URL
        assert ZoneLookupIngestionJob.SOURCE_URL.startswith("https://")

    def test_required_columns_constant(self):
        """Test REQUIRED_COLUMNS constant contains expected columns."""
        required = ZoneLookupIngestionJob.REQUIRED_COLUMNS
        assert "LocationID" in required
        assert "Borough" in required
        assert "Zone" in required
        assert "service_zone" in required
        assert len(required) == 4

    def test_minio_object_path_constant(self):
        """Test MINIO_OBJECT_PATH constant is set correctly."""
        assert ZoneLookupIngestionJob.MINIO_OBJECT_PATH == "misc/taxi_zone_lookup.csv"


class TestReferenceDataError:
    """Tests for ReferenceDataError exception class."""

    def test_reference_data_error_message(self):
        """Test ReferenceDataError stores message correctly."""
        error = ReferenceDataError("Reference data validation failed")
        assert str(error) == "Reference data validation failed"

    def test_reference_data_error_inheritance(self):
        """Test ReferenceDataError inherits from JobExecutionError."""
        error = ReferenceDataError("Test")
        assert isinstance(error, JobExecutionError)
        assert isinstance(error, Exception)

    def test_reference_data_error_with_cause(self):
        """Test ReferenceDataError can be raised with cause."""
        original = ValueError("Missing column")
        try:
            raise ReferenceDataError("Validation failed") from original
        except ReferenceDataError as e:
            assert e.__cause__ is original
