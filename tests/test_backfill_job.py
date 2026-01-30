"""
Tests for taxi_injection_safe_backfill_job to improve coverage.
"""

from etl.jobs.bronze.taxi_injection_safe_backfill_job import parse_month_spec


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
