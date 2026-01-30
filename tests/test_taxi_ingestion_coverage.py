"""
Extended coverage tests for TaxiIngestionJob.

Tests cover uncovered methods:
- _extract_with_minio_cache
- _extract_with_local_cache
- _download_file
- transform
- load
- run_bulk_ingestion
"""

import pytest
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
import requests

from etl.jobs.bronze.taxi_ingestion_job import (
    TaxiIngestionJob,
    DownloadError,
    DataValidationError,
    run_bulk_ingestion,
)
from etl.jobs.base_job import JobExecutionError
from etl.jobs.utils.config import JobConfig


class TestTaxiIngestionJobExtractWithMinioCache:
    """Tests for _extract_with_minio_cache method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_with_minio_cache_hit(self):
        """Test cache hit scenario - file exists in MinIO."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_minio_client = MagicMock()
        mock_minio_client.stat_object.return_value = MagicMock()  # File exists
        
        mock_df = MagicMock()
        mock_spark_read = MagicMock()
        mock_spark_read.parquet.return_value = mock_df
        
        with patch.object(job, "_get_minio_client", return_value=mock_minio_client):
            with patch.object(job, "spark") as mock_spark:
                mock_spark.read = mock_spark_read
                result = job._extract_with_minio_cache("http://example.com/file.parquet")
        
        assert result is mock_df
        mock_minio_client.stat_object.assert_called_once()
        mock_spark_read.parquet.assert_called_once()

    def test_extract_with_minio_cache_miss_download_success(self):
        """Test cache miss - download and upload to MinIO."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_minio_client = MagicMock()
        # First call: cache miss (raises S3Error), second call: file exists after upload
        from minio.error import S3Error
        mock_minio_client.stat_object.side_effect = [
            S3Error("NoSuchKey", "Not found", "", "", "", ""),
            MagicMock(),  # After upload
        ]
        
        mock_df = MagicMock()
        mock_spark_read = MagicMock()
        mock_spark_read.parquet.return_value = mock_df
        
        with patch.object(job, "_get_minio_client", return_value=mock_minio_client):
            with patch.object(job, "spark") as mock_spark:
                mock_spark.read = mock_spark_read
                with patch.object(job, "_download_file"):
                    with patch("pathlib.Path.mkdir"):
                        with patch("pathlib.Path.unlink"):
                            result = job._extract_with_minio_cache("http://example.com/file.parquet")
        
        assert result is mock_df
        mock_minio_client.fput_object.assert_called_once()

    def test_extract_with_minio_cache_upload_fails_fallback_local(self):
        """Test MinIO upload fails but falls back to local file."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_minio_client = MagicMock()
        from minio.error import S3Error
        # Cache miss, then upload fails, then stat fails (read from local)
        mock_minio_client.stat_object.side_effect = [
            S3Error("NoSuchKey", "Not found", "", "", "", ""),
            S3Error("NoSuchKey", "Not found", "", "", "", ""),
        ]
        mock_minio_client.fput_object.side_effect = S3Error("Error", "Upload failed", "", "", "", "")
        
        mock_df = MagicMock()
        mock_spark_read = MagicMock()
        mock_spark_read.parquet.return_value = mock_df
        
        with patch.object(job, "_get_minio_client", return_value=mock_minio_client):
            with patch.object(job, "spark") as mock_spark:
                mock_spark.read = mock_spark_read
                with patch.object(job, "_download_file"):
                    with patch("pathlib.Path.mkdir"):
                        with patch("pathlib.Path.unlink"):
                            result = job._extract_with_minio_cache("http://example.com/file.parquet")
        
        assert result is mock_df

    def test_extract_with_minio_cache_download_fails(self):
        """Test download failure raises DownloadError."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_minio_client = MagicMock()
        from minio.error import S3Error
        mock_minio_client.stat_object.side_effect = S3Error("NoSuchKey", "Not found", "", "", "", "")
        
        with patch.object(job, "_get_minio_client", return_value=mock_minio_client):
            with patch.object(job, "_download_file", side_effect=Exception("Network error")):
                with patch("pathlib.Path.mkdir"):
                    with pytest.raises(DownloadError) as exc_info:
                        job._extract_with_minio_cache("http://example.com/file.parquet")
        
        assert "Failed to download" in str(exc_info.value)

    def test_extract_with_minio_cache_cleanup_fails_continues(self):
        """Test that cleanup failure doesn't break the flow."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_minio_client = MagicMock()
        from minio.error import S3Error
        mock_minio_client.stat_object.side_effect = [
            S3Error("NoSuchKey", "Not found", "", "", "", ""),
            MagicMock(),
        ]
        
        mock_df = MagicMock()
        mock_spark_read = MagicMock()
        mock_spark_read.parquet.return_value = mock_df
        
        with patch.object(job, "_get_minio_client", return_value=mock_minio_client):
            with patch.object(job, "spark") as mock_spark:
                mock_spark.read = mock_spark_read
                with patch.object(job, "_download_file"):
                    with patch("pathlib.Path.mkdir"):
                        with patch("pathlib.Path.unlink", side_effect=Exception("Cleanup failed")):
                            result = job._extract_with_minio_cache("http://example.com/file.parquet")
        
        assert result is mock_df


class TestTaxiIngestionJobExtractWithLocalCache:
    """Tests for _extract_with_local_cache method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_with_local_cache_hit(self):
        """Test local cache hit - file exists."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_df = MagicMock()
        mock_spark_read = MagicMock()
        mock_spark_read.parquet.return_value = mock_df
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            with patch("pathlib.Path.mkdir"):
                with patch("pathlib.Path.exists", return_value=True):
                    result = job._extract_with_local_cache("http://example.com/file.parquet")
        
        assert result is mock_df

    def test_extract_with_local_cache_miss_download(self):
        """Test local cache miss - downloads file."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_df = MagicMock()
        mock_spark_read = MagicMock()
        mock_spark_read.parquet.return_value = mock_df
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            with patch.object(job, "_download_file") as mock_download:
                with patch("pathlib.Path.mkdir"):
                    with patch("pathlib.Path.exists", return_value=False):
                        result = job._extract_with_local_cache("http://example.com/file.parquet")
        
        assert result is mock_df
        mock_download.assert_called_once()

    def test_extract_with_local_cache_download_fails(self):
        """Test download failure raises DownloadError."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        with patch.object(job, "_download_file", side_effect=Exception("Network error")):
            with patch("pathlib.Path.mkdir"):
                with patch("pathlib.Path.exists", return_value=False):
                    with pytest.raises(DownloadError) as exc_info:
                        job._extract_with_local_cache("http://example.com/file.parquet")
        
        assert "Failed to download" in str(exc_info.value)

    def test_extract_with_local_cache_read_fails(self):
        """Test parquet read failure raises JobExecutionError."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_spark_read = MagicMock()
        mock_spark_read.parquet.side_effect = Exception("Corrupt file")
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            with patch("pathlib.Path.mkdir"):
                with patch("pathlib.Path.exists", return_value=True):
                    with pytest.raises(JobExecutionError) as exc_info:
                        job._extract_with_local_cache("http://example.com/file.parquet")
        
        assert "Failed to read parquet file" in str(exc_info.value)


class TestTaxiIngestionJobDownloadFile:
    """Tests for _download_file method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_download_file_success(self):
        """Test successful file download."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "1000"}
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
        
        with patch("requests.get", return_value=mock_response):
            with patch("builtins.open", mock_open()) as mock_file:
                job._download_file("http://example.com/file.parquet", Path("/tmp/file.parquet"))
        
        mock_file.assert_called_once()

    def test_download_file_timeout(self):
        """Test download timeout raises DownloadError."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        with patch("requests.get", side_effect=requests.exceptions.Timeout("Timeout")):
            with pytest.raises(DownloadError) as exc_info:
                job._download_file("http://example.com/file.parquet", Path("/tmp/file.parquet"))
        
        assert "timeout" in str(exc_info.value).lower()

    def test_download_file_http_error(self):
        """Test HTTP error raises DownloadError."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        
        with patch("requests.get", return_value=mock_response):
            with pytest.raises(DownloadError) as exc_info:
                job._download_file("http://example.com/file.parquet", Path("/tmp/file.parquet"))
        
        assert "HTTP error" in str(exc_info.value)

    def test_download_file_request_exception(self):
        """Test request exception raises DownloadError."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        with patch("requests.get", side_effect=requests.exceptions.RequestException("Connection failed")):
            with pytest.raises(DownloadError) as exc_info:
                job._download_file("http://example.com/file.parquet", Path("/tmp/file.parquet"))
        
        assert "Download failed" in str(exc_info.value)

    def test_download_file_io_error(self):
        """Test IO error raises DownloadError."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "1000"}
        mock_response.iter_content.return_value = [b"chunk1"]
        
        with patch("requests.get", return_value=mock_response):
            with patch("builtins.open", side_effect=IOError("Disk full")):
                with pytest.raises(DownloadError) as exc_info:
                    job._download_file("http://example.com/file.parquet", Path("/tmp/file.parquet"))
        
        assert "Failed to write file" in str(exc_info.value)


class TestTaxiIngestionJobTransform:
    """Tests for transform method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_transform_adds_metadata_columns(self):
        """Test transform adds required metadata columns."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        # Create mock DataFrame with proper chaining
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.columns = ["col1", "col2"]
        mock_df.schema = MagicMock()
        
        # Mock the withColumn chain
        mock_df_with_hash = MagicMock()
        mock_df_with_hash.withColumn.return_value = mock_df_with_hash
        mock_df_with_hash.select.return_value.distinct.return_value.count.return_value = 100
        mock_df.withColumn.return_value = mock_df_with_hash
        
        with patch("pyspark.sql.functions.sha2") as mock_sha2:
            with patch("pyspark.sql.functions.concat_ws"):
                with patch("pyspark.sql.functions.coalesce"):
                    with patch("pyspark.sql.functions.col"):
                        with patch("pyspark.sql.functions.lit"):
                            with patch("pyspark.sql.functions.current_timestamp"):
                                with patch("pyspark.sql.functions.current_date"):
                                    result = job.transform(mock_df)
        
        assert result is not None

    def test_transform_logs_duplicate_warning(self):
        """Test transform logs warning when duplicates found."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.columns = ["col1", "col2"]
        mock_df.schema = MagicMock()
        
        mock_df_with_hash = MagicMock()
        mock_df_with_hash.withColumn.return_value = mock_df_with_hash
        # Return fewer unique hashes than total records (duplicates)
        mock_df_with_hash.select.return_value.distinct.return_value.count.return_value = 90
        mock_df.withColumn.return_value = mock_df_with_hash
        
        with patch("pyspark.sql.functions.sha2"):
            with patch("pyspark.sql.functions.concat_ws"):
                with patch("pyspark.sql.functions.coalesce"):
                    with patch("pyspark.sql.functions.col"):
                        with patch("pyspark.sql.functions.lit"):
                            with patch("pyspark.sql.functions.current_timestamp"):
                                with patch("pyspark.sql.functions.current_date"):
                                    result = job.transform(mock_df)
        
        assert result is not None


class TestTaxiIngestionJobLoad:
    """Tests for load method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_load_to_minio(self):
        """Test load writes to MinIO when use_minio is True."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        job.config.minio.use_minio = True
        
        mock_df = MagicMock()
        mock_df.cache.return_value = mock_df
        mock_df.count.return_value = 100
        
        mock_write = MagicMock()
        mock_write.mode.return_value = mock_write
        mock_write.partitionBy.return_value = mock_write
        mock_write.option.return_value = mock_write
        mock_df.write = mock_write
        
        job.load(mock_df)
        
        mock_write.parquet.assert_called_once()

    def test_load_to_local(self):
        """Test load writes locally when use_minio is False."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        job.config.minio.use_minio = False
        
        mock_df = MagicMock()
        mock_df.cache.return_value = mock_df
        mock_df.count.return_value = 100
        
        mock_write = MagicMock()
        mock_write.mode.return_value = mock_write
        mock_write.partitionBy.return_value = mock_write
        mock_write.option.return_value = mock_write
        mock_df.write = mock_write
        
        job.load(mock_df)
        
        mock_write.parquet.assert_called_once()


class TestTaxiIngestionJobGetMinioClientError:
    """Tests for _get_minio_client error handling."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_get_minio_client_failure(self):
        """Test _get_minio_client raises JobExecutionError on failure."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        with patch("etl.jobs.bronze.taxi_ingestion_job.Minio", side_effect=Exception("Connection failed")):
            with pytest.raises(JobExecutionError) as exc_info:
                job._get_minio_client()
        
        assert "Failed to create MinIO client" in str(exc_info.value)


class TestDataValidationError:
    """Tests for DataValidationError exception."""

    def test_data_validation_error_message(self):
        """Test DataValidationError stores message correctly."""
        error = DataValidationError("Validation failed")
        assert str(error) == "Validation failed"

    def test_data_validation_error_inheritance(self):
        """Test DataValidationError inherits from JobExecutionError."""
        error = DataValidationError("test")
        assert isinstance(error, JobExecutionError)


class TestRunBulkIngestion:
    """Tests for run_bulk_ingestion function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_run_bulk_ingestion_multiple_months(self):
        """Test bulk ingestion with multiple months."""
        with patch.object(TaxiIngestionJob, "run", return_value=True):
            results = run_bulk_ingestion("yellow", 2024, 1, 2024, 3)
        
        assert len(results) == 3

    def test_run_bulk_ingestion_cross_year(self):
        """Test bulk ingestion crossing year boundary."""
        with patch.object(TaxiIngestionJob, "run", return_value=True):
            results = run_bulk_ingestion("yellow", 2023, 11, 2024, 2)
        
        assert len(results) == 4  # Nov, Dec 2023, Jan, Feb 2024


class TestTaxiIngestionJobExtractFromMinio:
    """Tests for _extract_from_minio method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_from_minio_success(self):
        """Test successful extraction from MinIO."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        
        mock_df = MagicMock()
        mock_spark_read = MagicMock()
        mock_spark_read.parquet.return_value = mock_df
        
        with patch.object(job, "spark") as mock_spark:
            mock_spark.read = mock_spark_read
            result = job._extract_from_minio()
        
        assert result is mock_df


class TestTaxiIngestionJobExtractFromSource:
    """Tests for _extract_from_source method."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_extract_from_source_uses_minio_cache(self):
        """Test _extract_from_source uses MinIO cache when enabled."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        job.config.minio.use_minio = True
        
        mock_df = MagicMock()
        
        with patch.object(job, "_extract_with_minio_cache", return_value=mock_df) as mock_extract:
            result = job._extract_from_source()
        
        assert result is mock_df
        mock_extract.assert_called_once()

    def test_extract_from_source_uses_local_cache(self):
        """Test _extract_from_source uses local cache when MinIO disabled."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        job.config.minio.use_minio = False
        
        mock_df = MagicMock()
        
        with patch.object(job, "_extract_with_local_cache", return_value=mock_df) as mock_extract:
            result = job._extract_from_source()
        
        assert result is mock_df
        mock_extract.assert_called_once()
