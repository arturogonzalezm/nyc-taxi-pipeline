"""
Safe Historical Backfill Script

This script demonstrates:
1. Processing multiple historical months
2. Re-running or backfilling specific months
3. Avoiding duplicates and data corruption

Features:
- Checks for existing partitions
- Deletes before re-ingest (prevents duplicates)
- Validates results after bronze
- Detects and reports duplicates
- Comprehensive logging
- Summary report

Usage:
    # Backfill specific months
    python etl/jobs/bronze/taxi_injection_safe_backfill_job.py yellow 2023-03 2023-07 2023-11

    # Backfill a range
    python etl/jobs/bronze/taxi_injection_safe_backfill_job.py yellow 2023-01:2023-12

    # Skip deletion (keep existing)
    python etl/jobs/bronze/taxi_injection_safe_backfill_job.py yellow 2024-01 --no-delete
"""

import sys
import logging
from pathlib import Path
from typing import List, Tuple, Dict, Any
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_month_spec(spec: str) -> List[Tuple[int, int]]:
    """
    Parse month specification into list of (year, month) tuples.
    :params spec: Month specification like "2023-03" or "2023-01:2023-12"
    :returns: List of (year, month) tuples

    Examples:
        "2023-03" -> [(2023, 3)]
        "2023-01:2023-03" -> [(2023, 1), (2023, 2), (2023, 3)]
    """
    if ":" in spec:
        # Range specification
        start, end = spec.split(":")
        start_year, start_month = map(int, start.split("-"))
        end_year, end_month = map(int, end.split("-"))

        from datetime import date
        from dateutil.relativedelta import relativedelta

        current = date(start_year, start_month, 1)
        end_date = date(end_year, end_month, 1)

        months = []
        while current <= end_date:
            months.append((current.year, current.month))
            current += relativedelta(months=1)

        return months
    else:
        # Single month
        year, month = map(int, spec.split("-"))
        return [(year, month)]


def check_partition_exists(
    spark, taxi_type: str, year: int, month: int
) -> Tuple[bool, int, int]:
    """
    Check if partition exists and return statistics.
    :returns (exists, total_records, unique_records)
    """
    partition_path = (
        f"s3a://nyc-taxi-pipeline/bronze/nyc_taxi/{taxi_type}/year={year}/month={month}"
    )

    try:
        df = spark.read.parquet(partition_path)
        total_records = df.count()
        unique_records = df.select("record_hash").distinct().count()
        return True, total_records, unique_records
    except Exception:
        return False, 0, 0


def delete_partition(spark, taxi_type: str, year: int, month: int) -> bool:
    """
    Delete a partition from MinIO/S3.
    :returns: True if deleted successfully
    """
    partition_path = (
        f"s3a://nyc-taxi-pipeline/bronze/nyc_taxi/{taxi_type}/year={year}/month={month}"
    )

    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(partition_path), hadoop_conf
        )
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(partition_path)

        if fs.exists(path_obj):
            fs.delete(path_obj, True)  # True = recursive
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to delete partition: {e}")
        return False


def safe_backfill_month(
    spark, taxi_type: str, year: int, month: int, delete_existing: bool
) -> Dict[str, Any]:
    """
    Safely backfill a single month.
    :returns: Dictionary with results
    """
    from etl.jobs.bronze.taxi_ingestion_job import TaxiIngestionJob

    period = f"{year}-{month:02d}"
    result = {
        "period": period,
        "status": "UNKNOWN",
        "records_before": 0,
        "records_after": 0,
        "duplicates_before": 0,
        "duplicates_after": 0,
        "error": None,
    }

    logger.info("\n" + "=" * 80)
    logger.info(f"Processing {taxi_type} taxi - {period}")
    logger.info("=" * 80)

    # Check existing partition
    exists, total_before, unique_before = check_partition_exists(
        spark, taxi_type, year, month
    )

    if exists:
        duplicates_before = total_before - unique_before
        result["records_before"] = total_before
        result["duplicates_before"] = duplicates_before

        logger.info("📊 Existing partition found:")
        logger.info(f"   Total records:  {total_before:,}")
        logger.info(f"   Unique records: {unique_before:,}")
        logger.info(f"   Duplicates:     {duplicates_before:,}")

        if duplicates_before > 0:
            logger.warning(
                f"⚠️  Found {duplicates_before:,} duplicates in existing data"
            )

        if delete_existing:
            logger.info("🗑️  Deleting existing partition...")
            if delete_partition(spark, taxi_type, year, month):
                logger.info("✅ Partition deleted")
            else:
                logger.error("❌ Failed to delete partition")
                result["status"] = "ERROR"
                result["error"] = "Failed to delete existing partition"
                return result
        else:
            logger.info("⏭️  Skipping (delete_existing=False)")
            result["status"] = "SKIPPED"
            return result
    else:
        logger.info("ℹ️  No existing partition (first run)")

    # Run bronze
    logger.info("🚀 Starting bronze...")

    try:
        job = TaxiIngestionJob(taxi_type, year, month)
        success = job.run()

        if success:
            # Verify results
            exists_after, total_after, unique_after = check_partition_exists(
                spark, taxi_type, year, month
            )

            if not exists_after:
                logger.error("❌ Partition not found after bronze!")
                result["status"] = "FAILED"
                result["error"] = "Partition not found after bronze"
                return result

            duplicates_after = total_after - unique_after
            result["records_after"] = total_after
            result["duplicates_after"] = duplicates_after

            logger.info("✅ Ingestion successful:")
            logger.info(f"   Total records:  {total_after:,}")
            logger.info(f"   Unique records: {unique_after:,}")
            logger.info(f"   Duplicates:     {duplicates_after:,}")

            if duplicates_after > 0:
                logger.warning(
                    f"⚠️  Found {duplicates_after:,} duplicates after bronze!"
                )
                logger.warning(
                    "    This suggests multiple runs - consider investigating"
                )

            # Compare with before
            if exists:
                diff = total_after - total_before
                diff_pct = (diff / total_before * 100) if total_before > 0 else 0
                logger.info(
                    f"   Change from before: {diff:+,} records ({diff_pct:+.1f}%)"
                )

            # Get metrics
            metrics = job.get_metrics()
            logger.info("⏱️  Execution time:")
            logger.info(
                f"   Extract:   {metrics.get('extract_duration_seconds', 0):.2f}s"
            )
            logger.info(
                f"   Transform: {metrics.get('transform_duration_seconds', 0):.2f}s"
            )
            logger.info(f"   Load:      {metrics.get('load_duration_seconds', 0):.2f}s")
            logger.info(
                f"   Total:     {metrics.get('total_duration_seconds', 0):.2f}s"
            )

            result["status"] = "SUCCESS"

        else:
            logger.error("❌ Ingestion failed")
            result["status"] = "FAILED"
            result["error"] = "Job execution returned False"

    except Exception as e:
        logger.error(f"❌ Error during bronze: {e}")
        result["status"] = "ERROR"
        result["error"] = str(e)

    return result


def safe_historical_backfill(
    taxi_type: str, months: List[Tuple[int, int]], delete_existing: bool = True
) -> Dict[str, Dict]:
    """
    Safely backfill multiple historical months.
    :params taxi_type: "yellow" or "green"
    :params months: List of (year, month) tuples
    :params delete_existing: If True, deletes existing partitions before re-ingest
    :returns: Dictionary with results for each month
    """
    from etl.jobs.utils.spark_manager import SparkSessionManager

    logger.info("\n" + "#" * 80)
    logger.info("SAFE HISTORICAL BACKFILL")
    logger.info("#" * 80)
    logger.info(f"Taxi type:       {taxi_type}")
    logger.info(f"Months to process: {len(months)}")
    logger.info(f"Delete existing: {delete_existing}")
    logger.info(f"Start time:      {datetime.now()}")

    # Get Spark session
    spark = SparkSessionManager.get_session("HistoricalBackfill", enable_s3=True)

    results = {}

    for year, month in months:
        result = safe_backfill_month(spark, taxi_type, year, month, delete_existing)
        results[result["period"]] = result

    # Summary
    logger.info("\n" + "#" * 80)
    logger.info("BACKFILL SUMMARY")
    logger.info("#" * 80)

    successful = sum(1 for r in results.values() if r["status"] == "SUCCESS")
    failed = sum(1 for r in results.values() if r["status"] in ["FAILED", "ERROR"])
    skipped = sum(1 for r in results.values() if r["status"] == "SKIPPED")

    logger.info(f"Total months:  {len(results)}")
    logger.info(f"Successful:    {successful} ✅")
    logger.info(f"Failed:        {failed} ❌")
    logger.info(f"Skipped:       {skipped} ⏭️")
    logger.info(f"End time:      {datetime.now()}")
    logger.info("")

    # Detailed results
    logger.info("Detailed Results:")
    logger.info("-" * 80)

    for period, result in results.items():
        status = result["status"]
        status_emoji = {
            "SUCCESS": "✅",
            "FAILED": "❌",
            "ERROR": "❌",
            "SKIPPED": "⏭️",
            "UNKNOWN": "❓",
        }.get(status, "❓")

        logger.info(f"{status_emoji} {period}: {status}")

        if status == "SUCCESS":
            logger.info(f"   Records: {result['records_after']:,}")
            if result["duplicates_after"] > 0:
                logger.info(f"   ⚠️  Duplicates: {result['duplicates_after']:,}")

        elif status in ["FAILED", "ERROR"]:
            logger.info(f"   Error: {result.get('error', 'Unknown')}")

        elif status == "SKIPPED":
            logger.info(f"   Existing records: {result['records_before']:,}")

    # Duplicate summary
    total_duplicates = sum(
        r["duplicates_after"] for r in results.values() if r["status"] == "SUCCESS"
    )
    if total_duplicates > 0:
        logger.warning(f"\n⚠️  TOTAL DUPLICATES FOUND: {total_duplicates:,}")
        logger.warning("   Recommend investigating duplicate sources")

    return results


def main():
    """
    Main entry point for command-line usage.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Safe Historical Backfill Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Examples:
          # Backfill specific months
          python etl/jobs/bronze/taxi_injection_safe_backfill_job.py yellow 2023-03 2023-07 2023-11

          # Backfill a range
          python etl/jobs/bronze/taxi_injection_safe_backfill_job.py yellow 2023-01:2023-12

          # Skip deletion (keep existing, only process missing)
          python etl/jobs/bronze/taxi_injection_safe_backfill_job.py yellow 2024-01 2024-02 --no-delete

          # Multiple ranges
          python etl/jobs/bronze/taxi_injection_safe_backfill_job.py green 2023-01:2023-06 2023-11:2023-12
                """,
    )

    parser.add_argument(
        "taxi_type",
        type=str,
        choices=["yellow", "green"],
        help="Taxi type (yellow or green)",
    )

    parser.add_argument(
        "months",
        type=str,
        nargs="+",
        help="Month specifications (YYYY-MM or YYYY-MM:YYYY-MM for ranges)",
    )

    parser.add_argument(
        "--no-delete",
        action="store_true",
        help="Do not delete existing partitions (skip if exists)",
    )

    args = parser.parse_args()

    # Parse all month specifications
    all_months = []
    for spec in args.months:
        months = parse_month_spec(spec)
        all_months.extend(months)

    # Remove duplicates and sort
    all_months = sorted(set(all_months))

    logger.info(f"Parsed {len(all_months)} months to process:")
    for year, month in all_months[:5]:
        logger.info(f"  {year}-{month:02d}")
    if len(all_months) > 5:
        logger.info(f"  ... and {len(all_months) - 5} more")

    # Confirm
    if len(all_months) > 10:
        logger.info(f"\n⚠️  About to process {len(all_months)} months")
        response = input("Continue? (y/n): ")
        if response.lower() != "y":
            logger.info("Aborted by user")
            return

    # Run backfill
    delete_existing = not args.no_delete
    results = safe_historical_backfill(args.taxi_type, all_months, delete_existing)

    # Exit code
    failed = sum(1 for r in results.values() if r["status"] in ["FAILED", "ERROR"])
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
