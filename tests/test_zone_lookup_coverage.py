"""
Extended coverage tests for ZoneLookupIngestionJob.

Tests cover uncovered methods:
- _extract_from_source
- transform
- load
- _get_minio_client
- _ensure_bucket_exists
- run_zone_lookup_ingestion
"""

import pytest
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
import requests

from etl.jobs.bronze.zone_lookup_ingestion_job import (
    ZoneLookupIngestionJob,
    ReferenceDataError,
    run_zone_lookup_ingestion,
)
from etl.jobs.base_job import JobExecutionError
from etl.jobs.utils.config import JobConfig


class TestZoneLookupIngestionJobExtractFromSource:
    """Tests for _extract_from_source method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_from_source_cache_hit(self):
        """Test extraction when file exists in cache."""
        job = ZoneLookupIngestionJob()
        
        mock_df = MagicMock()
        mock_df.count.return_value = 265
        mock_spark_read = MagicMock()
        mock_spark_read.csv.return_value = mock_df
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            with patch("pathlib.Path.mkdir"):
                with patch("pathlib.Path.exists", return_value=True):
                    result = job._extract_from_source()
        
        assert result is mock_df
        mock_spark_read.csv.assert_called_once()

    def test_extract_from_source_cache_miss_download(self):
        """Test extraction downloads file when not in cache."""
        job = ZoneLookupIngestionJob()
        
        mock_df = MagicMock()
        mock_df.count.return_value = 265
        mock_spark_read = MagicMock()
        mock_spark_read.csv.return_value = mock_df
        
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "5000"}
        mock_response.iter_content.return_value = [b"data"]
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            with patch("pathlib.Path.mkdir"):
                with patch("pathlib.Path.exists", return_value=False):
                    with patch("requests.get", return_value=mock_response):
                        with patch("builtins.open", mock_open()):
                            result = job._extract_from_source()
        
        assert result is mock_df

    def test_extract_from_source_download_request_error(self):
        """Test extraction raises error on request failure."""
        job = ZoneLookupIngestionJob()
        
        with patch("pathlib.Path.mkdir"):
            with patch("pathlib.Path.exists", return_value=False):
                with patch("requests.get", side_effect=requests.exceptions.RequestException("Network error")):
                    with pytest.raises(JobExecutionError) as exc_info:
                        job._extract_from_source()
        
        assert "Failed to download zone lookup" in str(exc_info.value)

    def test_extract_from_source_io_error(self):
        """Test extraction raises error on IO failure."""
        job = ZoneLookupIngestionJob()
        
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "5000"}
        mock_response.iter_content.return_value = [b"data"]
        
        with patch("pathlib.Path.mkdir"):
            with patch("pathlib.Path.exists", return_value=False):
                with patch("requests.get", return_value=mock_response):
                    with patch("builtins.open", side_effect=IOError("Disk full")):
                        with pytest.raises(JobExecutionError) as exc_info:
                            job._extract_from_source()
        
        assert "Failed to write file" in str(exc_info.value)

    def test_extract_from_source_csv_read_error(self):
        """Test extraction raises error on CSV read failure."""
        job = ZoneLookupIngestionJob()
        
        mock_spark_read = MagicMock()
        mock_spark_read.csv.side_effect = Exception("Invalid CSV")
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            with patch("pathlib.Path.mkdir"):
                with patch("pathlib.Path.exists", return_value=True):
                    with pytest.raises(JobExecutionError) as exc_info:
                        job._extract_from_source()
        
        assert "Failed to read CSV" in str(exc_info.value)


class TestZoneLookupIngestionJobTransform:
    """Tests for transform method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_transform_missing_columns(self):
        """Test transform raises error for missing columns."""
        job = ZoneLookupIngestionJob()
        
        mock_df = MagicMock()
        mock_df.columns = ["LocationID", "Borough"]  # Missing Zone and service_zone
        mock_df.schema = MagicMock()
        
        with pytest.raises(ReferenceDataError) as exc_info:
            job.transform(mock_df)
        
        assert "Missing required columns" in str(exc_info.value)


class TestZoneLookupIngestionJobLoad:
    """Tests for load method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_load_to_minio(self):
        """Test load uploads to MinIO when enabled."""
        job = ZoneLookupIngestionJob()
        job.config.minio.use_minio = True
        
        mock_df = MagicMock()
        mock_df.count.return_value = 265
        
        mock_minio_client = MagicMock()
        
        with patch.object(job, "_get_minio_client", return_value=mock_minio_client):
            with patch.object(job, "_ensure_bucket_exists"):
                with patch("pathlib.Path.exists", return_value=True):
                    job.load(mock_df)
        
        mock_minio_client.fput_object.assert_called_once()

    def test_load_to_minio_file_not_found(self):
        """Test load raises error when local file not found."""
        job = ZoneLookupIngestionJob()
        job.config.minio.use_minio = True
        
        mock_df = MagicMock()
        mock_df.count.return_value = 265
        
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(FileNotFoundError) as exc_info:
                job.load(mock_df)
        
        assert "Local file not found" in str(exc_info.value)

    def test_load_to_minio_s3_error(self):
        """Test load raises error on S3 failure."""
        job = ZoneLookupIngestionJob()
        job.config.minio.use_minio = True
        
        mock_df = MagicMock()
        mock_df.count.return_value = 265
        
        mock_minio_client = MagicMock()
        from minio.error import S3Error
        mock_minio_client.fput_object.side_effect = S3Error("Error", "Upload failed", "", "", "", "")
        
        with patch.object(job, "_get_minio_client", return_value=mock_minio_client):
            with patch.object(job, "_ensure_bucket_exists"):
                with patch("pathlib.Path.exists", return_value=True):
                    with pytest.raises(JobExecutionError) as exc_info:
                        job.load(mock_df)
        
        assert "Failed to upload to MinIO" in str(exc_info.value)

    def test_load_to_minio_generic_error(self):
        """Test load raises error on generic failure."""
        job = ZoneLookupIngestionJob()
        job.config.minio.use_minio = True
        
        mock_df = MagicMock()
        mock_df.count.return_value = 265
        
        mock_minio_client = MagicMock()
        mock_minio_client.fput_object.side_effect = Exception("Unknown error")
        
        with patch.object(job, "_get_minio_client", return_value=mock_minio_client):
            with patch.object(job, "_ensure_bucket_exists"):
                with patch("pathlib.Path.exists", return_value=True):
                    with pytest.raises(JobExecutionError) as exc_info:
                        job.load(mock_df)
        
        assert "MinIO upload failed" in str(exc_info.value)

    def test_load_local_only(self):
        """Test load keeps file locally when MinIO disabled."""
        job = ZoneLookupIngestionJob()
        job.config.minio.use_minio = False
        
        mock_df = MagicMock()
        mock_df.count.return_value = 265
        
        # Should not raise, just log
        job.load(mock_df)


class TestZoneLookupIngestionJobGetMinioClient:
    """Tests for _get_minio_client method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_get_minio_client_success(self):
        """Test successful MinIO client creation."""
        job = ZoneLookupIngestionJob()
        
        mock_client = MagicMock()
        
        with patch("etl.jobs.bronze.zone_lookup_ingestion_job.Minio", return_value=mock_client):
            result = job._get_minio_client()
        
        assert result is mock_client

    def test_get_minio_client_strips_protocol(self):
        """Test MinIO client strips http:// from endpoint."""
        job = ZoneLookupIngestionJob()
        job.config.minio.endpoint = "http://localhost:9000"
        
        mock_client = MagicMock()
        
        with patch("etl.jobs.bronze.zone_lookup_ingestion_job.Minio", return_value=mock_client) as mock_minio:
            job._get_minio_client()
        
        # Verify endpoint was stripped of http://
        call_args = mock_minio.call_args
        assert "http://" not in call_args[0][0]

    def test_get_minio_client_failure(self):
        """Test MinIO client creation failure."""
        job = ZoneLookupIngestionJob()
        
        with patch("etl.jobs.bronze.zone_lookup_ingestion_job.Minio", side_effect=Exception("Connection failed")):
            with pytest.raises(JobExecutionError) as exc_info:
                job._get_minio_client()
        
        assert "Failed to create MinIO client" in str(exc_info.value)


class TestZoneLookupIngestionJobEnsureBucketExists:
    """Tests for _ensure_bucket_exists method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_ensure_bucket_exists_already_exists(self):
        """Test when bucket already exists."""
        job = ZoneLookupIngestionJob()
        
        mock_minio_client = MagicMock()
        mock_minio_client.bucket_exists.return_value = True
        
        job._ensure_bucket_exists(mock_minio_client, "test-bucket")
        
        mock_minio_client.make_bucket.assert_not_called()

    def test_ensure_bucket_exists_creates_bucket(self):
        """Test bucket creation when it doesn't exist."""
        job = ZoneLookupIngestionJob()
        
        mock_minio_client = MagicMock()
        mock_minio_client.bucket_exists.return_value = False
        
        job._ensure_bucket_exists(mock_minio_client, "test-bucket")
        
        mock_minio_client.make_bucket.assert_called_once_with("test-bucket")

    def test_ensure_bucket_exists_method_exists(self):
        """Test _ensure_bucket_exists method exists."""
        job = ZoneLookupIngestionJob()
        
        assert hasattr(job, "_ensure_bucket_exists")
        assert callable(job._ensure_bucket_exists)


class TestReferenceDataError:
    """Tests for ReferenceDataError exception."""

    def test_reference_data_error_message(self):
        """Test ReferenceDataError stores message correctly."""
        error = ReferenceDataError("Invalid reference data")
        assert str(error) == "Invalid reference data"

    def test_reference_data_error_inheritance(self):
        """Test ReferenceDataError inherits from JobExecutionError."""
        error = ReferenceDataError("test")
        assert isinstance(error, JobExecutionError)


class TestRunZoneLookupIngestion:
    """Tests for run_zone_lookup_ingestion function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_run_zone_lookup_ingestion_success(self):
        """Test successful zone lookup ingestion."""
        with patch.object(ZoneLookupIngestionJob, "run", return_value=True):
            result = run_zone_lookup_ingestion()
        
        assert result is True

    def test_run_zone_lookup_ingestion_failure(self):
        """Test failed zone lookup ingestion."""
        with patch.object(ZoneLookupIngestionJob, "run", return_value=False):
            result = run_zone_lookup_ingestion()
        
        assert result is False


class TestZoneLookupIngestionJobInit:
    """Tests for ZoneLookupIngestionJob initialization."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_init_sets_constants(self):
        """Test initialization sets class constants."""
        job = ZoneLookupIngestionJob()
        
        assert job.SOURCE_URL is not None
        assert job.REQUIRED_COLUMNS is not None
        assert "LocationID" in job.REQUIRED_COLUMNS

    def test_init_sets_file_name(self):
        """Test initialization sets file name."""
        job = ZoneLookupIngestionJob()
        
        assert job.file_name == "taxi_zone_lookup.csv"

    def test_init_sets_job_name(self):
        """Test initialization sets job name."""
        job = ZoneLookupIngestionJob()
        
        assert "ZoneLookup" in job.job_name


class TestZoneLookupIngestionJobValidateInputs:
    """Tests for validate_inputs method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_validate_inputs_passes(self):
        """Test validate_inputs passes for valid job."""
        job = ZoneLookupIngestionJob()
        
        # Should not raise
        job.validate_inputs()


class TestZoneLookupIngestionJobExtract:
    """Tests for extract method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_calls_extract_from_source(self):
        """Test extract calls _extract_from_source."""
        job = ZoneLookupIngestionJob()
        
        mock_df = MagicMock()
        
        with patch.object(job, "_extract_from_source", return_value=mock_df) as mock_extract:
            result = job.extract()
        
        assert result is mock_df
        mock_extract.assert_called_once()
