"""
Tests for ZoneLookupIngestionJob to improve coverage.
"""

from etl.jobs.bronze.zone_lookup_ingestion_job import (
    ZoneLookupIngestionJob,
    ReferenceDataError,
)
from etl.jobs.base_job import JobExecutionError


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
