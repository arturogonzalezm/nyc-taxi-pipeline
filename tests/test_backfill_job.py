"""
Tests for taxi_injection_safe_backfill_job to improve coverage.
"""


from unittest.mock import patch, MagicMock

from etl.jobs.bronze.taxi_injection_safe_backfill_job import (
    parse_month_spec,
    safe_historical_backfill,
)


class TestParseMonthSpec:
    """Tests for parse_month_spec utility function."""

    def test_single_month(self):
        """Test parsing a single month specification."""
        result = parse_month_spec("2023-03")
        assert result == [(2023, 3)]

    def test_single_month_january(self):
        """Test parsing January."""
        result = parse_month_spec("2024-01")
        assert result == [(2024, 1)]

    def test_single_month_december(self):
        """Test parsing December."""
        result = parse_month_spec("2023-12")
        assert result == [(2023, 12)]

    def test_range_same_year(self):
        """Test parsing a range within the same year."""
        result = parse_month_spec("2023-01:2023-03")
        assert result == [(2023, 1), (2023, 2), (2023, 3)]

    def test_range_cross_year(self):
        """Test parsing a range crossing year boundary."""
        result = parse_month_spec("2023-11:2024-02")
        assert result == [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]

    def test_range_single_month(self):
        """Test parsing a range with same start and end."""
        result = parse_month_spec("2023-06:2023-06")
        assert result == [(2023, 6)]

    def test_range_full_year(self):
        """Test parsing a full year range."""
        result = parse_month_spec("2023-01:2023-12")
        assert len(result) == 12
        assert result[0] == (2023, 1)
        assert result[-1] == (2023, 12)

    def test_range_multiple_years(self):
        """Test parsing a range spanning multiple years."""
        result = parse_month_spec("2022-06:2024-06")
        # 7 months in 2022 + 12 months in 2023 + 6 months in 2024 = 25 months
        assert len(result) == 25
        assert result[0] == (2022, 6)
        assert result[-1] == (2024, 6)

    def test_single_month_leading_zero(self):
        """Test parsing month with leading zero."""
        result = parse_month_spec("2023-05")
        assert result == [(2023, 5)]

    def test_range_preserves_order(self):
        """Test that range results are in chronological order."""
        result = parse_month_spec("2023-01:2023-06")
        for i in range(len(result) - 1):
            current_year, current_month = result[i]
            next_year, next_month = result[i + 1]
            # Next should be greater
            assert (next_year, next_month) > (current_year, current_month)


class TestSafeHistoricalBackfill:
    """Tests for safe_historical_backfill function."""

    def test_safe_historical_backfill_returns_dict(self):
        """Test that safe_historical_backfill returns a dictionary."""
        with patch(
            "etl.jobs.utils.spark_manager.SparkSessionManager"
        ) as mock_spark_manager:
            mock_spark = MagicMock()
            mock_spark_manager.get_session.return_value = mock_spark

            with patch(
                "etl.jobs.bronze.taxi_injection_safe_backfill_job.safe_backfill_month"
            ) as mock_backfill:
                mock_backfill.return_value = {
                    "period": "2024-01",
                    "status": "SUCCESS",
                    "records_before": 0,
                    "records_after": 1000,
                    "duplicates_before": 0,
                    "duplicates_after": 0,
                    "error": None,
                }

                result = safe_historical_backfill("yellow", [(2024, 1)])

                assert isinstance(result, dict)
                assert "2024-01" in result

    def test_safe_historical_backfill_processes_multiple_months(self):
        """Test that safe_historical_backfill processes multiple months."""
        with patch(
            "etl.jobs.utils.spark_manager.SparkSessionManager"
        ) as mock_spark_manager:
            mock_spark = MagicMock()
            mock_spark_manager.get_session.return_value = mock_spark

            with patch(
                "etl.jobs.bronze.taxi_injection_safe_backfill_job.safe_backfill_month"
            ) as mock_backfill:
                mock_backfill.side_effect = [
                    {
                        "period": "2024-01",
                        "status": "SUCCESS",
                        "records_before": 0,
                        "records_after": 1000,
                        "duplicates_before": 0,
                        "duplicates_after": 0,
                        "error": None,
                    },
                    {
                        "period": "2024-02",
                        "status": "SUCCESS",
                        "records_before": 0,
                        "records_after": 1200,
                        "duplicates_before": 0,
                        "duplicates_after": 0,
                        "error": None,
                    },
                ]

                result = safe_historical_backfill("yellow", [(2024, 1), (2024, 2)])

                assert len(result) == 2
                assert "2024-01" in result
                assert "2024-02" in result

    def test_safe_historical_backfill_with_delete_existing_false(self):
        """Test safe_historical_backfill with delete_existing=False."""
        with patch(
            "etl.jobs.utils.spark_manager.SparkSessionManager"
        ) as mock_spark_manager:
            mock_spark = MagicMock()
            mock_spark_manager.get_session.return_value = mock_spark

            with patch(
                "etl.jobs.bronze.taxi_injection_safe_backfill_job.safe_backfill_month"
            ) as mock_backfill:
                mock_backfill.return_value = {
                    "period": "2024-01",
                    "status": "SKIPPED",
                    "records_before": 1000,
                    "records_after": 0,
                    "duplicates_before": 0,
                    "duplicates_after": 0,
                    "error": None,
                }

                result = safe_historical_backfill(
                    "green", [(2024, 1)], delete_existing=False
                )

                assert result["2024-01"]["status"] == "SKIPPED"
