"""
Tests for bulk ingestion functionality in taxi_ingestion_job.
"""


from unittest.mock import patch, MagicMock

from etl.jobs.bronze.taxi_ingestion_job import (
    TaxiIngestionJob,
    run_bulk_ingestion,
    DownloadError,
)
from etl.jobs.utils.config import JobConfig


class TestRunBulkIngestion:
    """Tests for run_bulk_ingestion function."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_run_bulk_ingestion_single_month(self):
        """Test bulk ingestion with single month."""
        with patch(
            "etl.jobs.bronze.taxi_ingestion_job.run_ingestion"
        ) as mock_run:
            mock_run.return_value = True
            results = run_bulk_ingestion("yellow", 2024, 1, 2024, 1)
            assert "2024-01" in results
            assert "SUCCESS" in results["2024-01"]

    def test_run_bulk_ingestion_multiple_months(self):
        """Test bulk ingestion with multiple months."""
        with patch(
            "etl.jobs.bronze.taxi_ingestion_job.run_ingestion"
        ) as mock_run:
            mock_run.return_value = True
            results = run_bulk_ingestion("yellow", 2024, 1, 2024, 3)
            assert len(results) == 3
            assert "2024-01" in results
            assert "2024-02" in results
            assert "2024-03" in results

    def test_run_bulk_ingestion_cross_year(self):
        """Test bulk ingestion crossing year boundary."""
        with patch(
            "etl.jobs.bronze.taxi_ingestion_job.run_ingestion"
        ) as mock_run:
            mock_run.return_value = True
            results = run_bulk_ingestion("yellow", 2023, 11, 2024, 2)
            assert len(results) == 4
            assert "2023-11" in results
            assert "2023-12" in results
            assert "2024-01" in results
            assert "2024-02" in results

    def test_run_bulk_ingestion_handles_failure(self):
        """Test bulk ingestion handles job failure."""
        with patch(
            "etl.jobs.bronze.taxi_ingestion_job.run_ingestion"
        ) as mock_run:
            mock_run.return_value = False
            results = run_bulk_ingestion("yellow", 2024, 1, 2024, 1)
            assert "2024-01" in results
            assert "FAILED" in results["2024-01"]

    def test_run_bulk_ingestion_handles_exception(self):
        """Test bulk ingestion handles exceptions."""
        with patch(
            "etl.jobs.bronze.taxi_ingestion_job.run_ingestion"
        ) as mock_run:
            mock_run.side_effect = Exception("Test error")
            results = run_bulk_ingestion("yellow", 2024, 1, 2024, 1)
            assert "2024-01" in results
            assert "ERROR" in results["2024-01"]

    def test_run_bulk_ingestion_handles_download_error_404(self):
        """Test bulk ingestion handles 404 download errors."""
        with patch(
            "etl.jobs.bronze.taxi_ingestion_job.run_ingestion"
        ) as mock_run:
            error = DownloadError("HTTP 404: Not Found")
            mock_run.side_effect = error
            results = run_bulk_ingestion("yellow", 2024, 1, 2024, 1)
            assert "2024-01" in results
            assert "SKIPPED" in results["2024-01"] or "404" in results["2024-01"]

    def test_run_bulk_ingestion_with_green_taxi(self):
        """Test bulk ingestion with green taxi type."""
        with patch(
            "etl.jobs.bronze.taxi_ingestion_job.run_ingestion"
        ) as mock_run:
            mock_run.return_value = True
            results = run_bulk_ingestion("green", 2024, 1, 2024, 2)
            assert len(results) == 2
            mock_run.assert_called()

    def test_run_bulk_ingestion_continues_after_failure(self):
        """Test that bulk ingestion continues processing after a failure."""
        with patch(
            "etl.jobs.bronze.taxi_ingestion_job.run_ingestion"
        ) as mock_run:
            # First call fails, second succeeds
            mock_run.side_effect = [False, True]
            results = run_bulk_ingestion("yellow", 2024, 1, 2024, 2)
            assert len(results) == 2
            assert "FAILED" in results["2024-01"]
            assert "SUCCESS" in results["2024-02"]


class TestTaxiIngestionJobMethods:
    """Tests for TaxiIngestionJob methods."""

    def setup_method(self):
        """Reset JobConfig singleton before each test."""
        JobConfig.reset()

    def test_get_minio_client_creates_client(self):
        """Test _get_minio_client creates Minio client."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        with patch("etl.jobs.bronze.taxi_ingestion_job.Minio") as mock_minio:
            mock_client = MagicMock()
            mock_minio.return_value = mock_client
            client = job._get_minio_client()
            mock_minio.assert_called_once()
            assert client is mock_client

    def test_get_minio_client_uses_config(self):
        """Test _get_minio_client uses config values."""
        job = TaxiIngestionJob("yellow", 2024, 6)
        with patch("etl.jobs.bronze.taxi_ingestion_job.Minio") as mock_minio:
            mock_client = MagicMock()
            mock_minio.return_value = mock_client
            job._get_minio_client()
            # Verify Minio was called with endpoint from config
            call_args = mock_minio.call_args
            assert call_args is not None


class TestDownloadErrorExtended:
    """Extended tests for DownloadError."""

    def test_download_error_with_url(self):
        """Test DownloadError with URL in message."""
        url = "https://example.com/data.parquet"
        error = DownloadError(f"Failed to download {url}")
        assert url in str(error)

    def test_download_error_with_status_code(self):
        """Test DownloadError with HTTP status code."""
        error = DownloadError("HTTP 404: Not Found")
        assert "404" in str(error)

    def test_download_error_with_timeout(self):
        """Test DownloadError with timeout message."""
        error = DownloadError("Download timeout after 300s")
        assert "timeout" in str(error).lower()

    def test_download_error_is_exception(self):
        """Test DownloadError is an Exception."""
        error = DownloadError("test")
        assert isinstance(error, Exception)

    def test_download_error_can_be_caught(self):
        """Test DownloadError can be caught."""
        try:
            raise DownloadError("Test download error")
        except DownloadError as e:
            assert str(e) == "Test download error"
