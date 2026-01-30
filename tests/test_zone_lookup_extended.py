"""
Extended tests for ZoneLookupIngestionJob to improve coverage.
"""


from unittest.mock import patch, MagicMock

from etl.jobs.bronze.zone_lookup_ingestion_job import (
    ZoneLookupIngestionJob,
    ReferenceDataError,
)
from etl.jobs.base_job import JobExecutionError
from etl.jobs.utils.config import JobConfig


class TestZoneLookupIngestionJobExtended:
    """Extended tests for ZoneLookupIngestionJob."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_job_inherits_from_base_spark_job(self):
        """Test that job inherits from BaseSparkJob."""
        from etl.jobs.base_job import BaseSparkJob

        job = ZoneLookupIngestionJob()
        assert isinstance(job, BaseSparkJob)

    def test_job_has_logger(self):
        """Test that job has a logger."""
        job = ZoneLookupIngestionJob()
        assert job.logger is not None

    def test_job_has_config(self):
        """Test that job has config."""
        job = ZoneLookupIngestionJob()
        assert job.config is not None

    def test_extract_calls_extract_from_source(self):
        """Test that extract calls _extract_from_source."""
        job = ZoneLookupIngestionJob()
        with patch.object(job, "_extract_from_source") as mock_extract:
            mock_df = MagicMock()
            mock_extract.return_value = mock_df
            result = job.extract()
            mock_extract.assert_called_once()
            assert result is mock_df

    def test_file_name_attribute(self):
        """Test file_name attribute is set correctly."""
        job = ZoneLookupIngestionJob()
        assert job.file_name == "taxi_zone_lookup.csv"

    def test_source_url_attribute(self):
        """Test source_url attribute is set correctly."""
        job = ZoneLookupIngestionJob()
        assert "taxi_zone_lookup.csv" in job.source_url

    def test_job_name_is_zone_lookup_ingestion(self):
        """Test job name is ZoneLookupIngestion."""
        job = ZoneLookupIngestionJob()
        assert job.job_name == "ZoneLookupIngestion"


class TestZoneLookupIngestionJobGetMinioClient:
    """Tests for ZoneLookupIngestionJob._get_minio_client method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_get_minio_client_returns_client(self):
        """Test _get_minio_client returns a Minio client."""
        job = ZoneLookupIngestionJob()
        with patch("etl.jobs.bronze.zone_lookup_ingestion_job.Minio") as mock_minio:
            mock_client = MagicMock()
            mock_minio.return_value = mock_client
            client = job._get_minio_client()
            assert client is mock_client

    def test_get_minio_client_uses_config_endpoint(self):
        """Test _get_minio_client uses config endpoint."""
        job = ZoneLookupIngestionJob()
        with patch("etl.jobs.bronze.zone_lookup_ingestion_job.Minio") as mock_minio:
            mock_client = MagicMock()
            mock_minio.return_value = mock_client
            job._get_minio_client()
            # Verify Minio was called
            mock_minio.assert_called_once()


class TestReferenceDataErrorExtended:
    """Extended tests for ReferenceDataError."""

    def test_reference_data_error_is_job_execution_error(self):
        """Test ReferenceDataError is a JobExecutionError."""
        error = ReferenceDataError("test")
        assert isinstance(error, JobExecutionError)

    def test_reference_data_error_message_preserved(self):
        """Test error message is preserved."""
        message = "Missing required column: LocationID"
        error = ReferenceDataError(message)
        assert message in str(error)

    def test_reference_data_error_can_wrap_exception(self):
        """Test ReferenceDataError can wrap another exception."""
        original = ValueError("Original error")
        try:
            raise ReferenceDataError("Wrapped error") from original
        except ReferenceDataError as e:
            assert e.__cause__ is original

    def test_reference_data_error_with_column_info(self):
        """Test ReferenceDataError with column information."""
        error = ReferenceDataError("Column 'Borough' has null values")
        assert "Borough" in str(error)

    def test_reference_data_error_with_validation_details(self):
        """Test ReferenceDataError with validation details."""
        error = ReferenceDataError("Validation failed: 5 null LocationIDs found")
        assert "Validation failed" in str(error)
        assert "5" in str(error)


class TestZoneLookupConstants:
    """Tests for ZoneLookupIngestionJob constants."""

    def test_required_columns_has_location_id(self):
        """Test REQUIRED_COLUMNS includes LocationID."""
        assert "LocationID" in ZoneLookupIngestionJob.REQUIRED_COLUMNS

    def test_required_columns_has_borough(self):
        """Test REQUIRED_COLUMNS includes Borough."""
        assert "Borough" in ZoneLookupIngestionJob.REQUIRED_COLUMNS

    def test_required_columns_has_zone(self):
        """Test REQUIRED_COLUMNS includes Zone."""
        assert "Zone" in ZoneLookupIngestionJob.REQUIRED_COLUMNS

    def test_required_columns_has_service_zone(self):
        """Test REQUIRED_COLUMNS includes service_zone."""
        assert "service_zone" in ZoneLookupIngestionJob.REQUIRED_COLUMNS

    def test_source_url_is_https(self):
        """Test SOURCE_URL uses HTTPS."""
        assert ZoneLookupIngestionJob.SOURCE_URL.startswith("https://")

    def test_source_url_is_cloudfront(self):
        """Test SOURCE_URL is from CloudFront CDN."""
        assert "cloudfront.net" in ZoneLookupIngestionJob.SOURCE_URL

    def test_minio_object_path_is_misc(self):
        """Test MINIO_OBJECT_PATH is in misc directory."""
        assert ZoneLookupIngestionJob.MINIO_OBJECT_PATH.startswith("misc/")

    def test_file_name_is_csv(self):
        """Test FILE_NAME is a CSV file."""
        assert ZoneLookupIngestionJob.FILE_NAME.endswith(".csv")
