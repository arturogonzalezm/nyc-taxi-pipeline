"""
NYC Taxi Data Ingestion Job
Ingests taxi trip data from NYC TLC to MinIO bronze layer.
"""
import os
import requests
from pathlib import Path
from typing import Literal
from pyspark.sql import DataFrame

from ..base_job import BaseSparkJob
from ..utils.config import JobConfig


class TaxiIngestionJob(BaseSparkJob):
    """
    Ingestion job for NYC Taxi trip data.
    Downloads data from NYC TLC and stores it in MinIO bronze layer.
    """

    def __init__(
        self,
        taxi_type: Literal["yellow", "green"],
        year: int,
        month: int,
        config: JobConfig = None
    ):
        """
        Initialize the ingestion job.

        Args:
            taxi_type: Type of taxi (yellow or green)
            year: Year of the data
            month: Month of the data
            config: Job configuration
        """
        super().__init__(
            job_name=f"TaxiIngestion_{taxi_type}_{year}_{month:02d}",
            config=config
        )
        self.taxi_type = taxi_type
        self.year = year
        self.month = month
        self.file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"

    def validate_inputs(self):
        """Validate job inputs"""
        if self.taxi_type not in ["yellow", "green"]:
            raise ValueError(f"Invalid taxi type: {self.taxi_type}")

        if not 1 <= self.month <= 12:
            raise ValueError(f"Invalid month: {self.month}")

        if self.year < 2009:
            raise ValueError(f"Invalid year: {self.year}")

        self.logger.info(f"Validated inputs: {self.taxi_type}, {self.year}-{self.month:02d}")

    def extract(self) -> DataFrame:
        """
        Extract data from source.
        For ingestion job, always download from NYC TLC to get fresh data.
        """
        return self._extract_from_source()

    def _extract_from_minio(self) -> DataFrame:
        """Load data from MinIO bronze layer (used by downstream jobs)"""
        s3_path = self.config.get_s3_path("bronze", taxi_type=self.taxi_type)
        self.logger.info(f"Loading from MinIO: {s3_path}")
        # Read with partition filters for performance
        partition_path = f"{s3_path}/year={self.year}/month={self.month}"
        return self.spark.read.parquet(partition_path)

    def _extract_from_source(self) -> DataFrame:
        """Download and load data from NYC TLC"""
        # NYC TLC provides monthly parquet files via their CDN
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{self.file_name}"

        cache_path = Path(self.config.cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        local_file = cache_path / self.file_name

        if not local_file.exists():
            self.logger.info(f"Downloading from NYC TLC: {url}")

            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Download parquet file
            with open(local_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            self.logger.info(f"Downloaded to {local_file}")
        else:
            self.logger.info(f"Using cached file: {local_file}")

        return self.spark.read.parquet(str(local_file))

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform the data with minimal changes (Bronze layer principle).

        Bronze Layer Transformations:
        1. Preserve raw data - no business logic transformations
        2. Schema enforcement - validate and enforce data types
        3. Add metadata columns for lineage and CDC tracking
        4. Data quality validation - filter to requested year/month

        This ensures we maintain the raw, unaltered source data while adding
        necessary metadata for downstream processing and auditing.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import TimestampType, DoubleType, IntegerType

        record_count = df.count()
        self.logger.info(f"Processing {record_count:,} records before validation")
        self.logger.info(f"Schema: {df.schema}")

        # STEP 1: Schema enforcement - ensure critical columns have correct types
        # Identify the pickup datetime column (varies by taxi type)
        pickup_col = None
        if "tpep_pickup_datetime" in df.columns:
            pickup_col = "tpep_pickup_datetime"
        elif "lpep_pickup_datetime" in df.columns:
            pickup_col = "lpep_pickup_datetime"
        else:
            raise ValueError("No pickup datetime column found in data")

        # Enforce datetime type on pickup column if not already
        if df.schema[pickup_col].dataType != TimestampType():
            self.logger.warning(f"Converting {pickup_col} to timestamp type")
            df = df.withColumn(pickup_col, F.col(pickup_col).cast(TimestampType()))

        # STEP 2: Data quality validation - extract year/month for filtering
        df = df.withColumn("pickup_year", F.year(F.col(pickup_col))) \
               .withColumn("pickup_month", F.month(F.col(pickup_col)))

        # Log data distribution before filtering
        year_month_dist = df.groupBy("pickup_year", "pickup_month").count().collect()
        self.logger.info(f"Data distribution by year/month: {year_month_dist}")

        # Filter to only include records matching the requested year and month
        df_filtered = df.filter(
            (F.col("pickup_year") == self.year) &
            (F.col("pickup_month") == self.month)
        )

        filtered_count = df_filtered.count()
        self.logger.info(f"Filtered to {filtered_count:,} records for {self.year}-{self.month:02d}")

        if filtered_count == 0:
            raise ValueError(
                f"No records found for {self.year}-{self.month:02d}. "
                f"Source file may contain incorrect data."
            )

        # Validate that we're getting reasonable data
        match_percentage = (filtered_count / record_count) * 100 if record_count > 0 else 0
        self.logger.info(f"Match percentage: {match_percentage:.1f}%")

        if match_percentage < 50:
            self.logger.warning(
                f"Low match percentage ({match_percentage:.1f}%). "
                f"Expected more records for {self.year}-{self.month:02d}"
            )

        # STEP 3: Add metadata columns ONLY (no business logic transformations)
        # These metadata columns enable:
        # - Data lineage tracking (source_file, ingestion_timestamp)
        # - CDC operations (record_hash, ingestion_date)
        # - Partitioning (year, month)
        df_final = df_filtered.withColumn("ingestion_timestamp", F.current_timestamp()) \
                              .withColumn("ingestion_date", F.current_date()) \
                              .withColumn("source_file", F.lit(self.file_name)) \
                              .withColumn("year", F.lit(self.year)) \
                              .withColumn("month", F.lit(self.month)) \
                              .withColumn("record_hash", F.hash(*df.columns))  # For CDC change detection

        # Drop temporary columns used for validation
        df_final = df_final.drop("pickup_year", "pickup_month")

        self.logger.info(f"Bronze layer transformation complete: {filtered_count:,} records")
        self.logger.info("Transformations applied: schema enforcement, metadata addition only")

        return df_final

    def load(self, df: DataFrame):
        """
        Load data to MinIO bronze layer with partitioning.
        Partitioned by year and month for:
        - Delta Lake compatibility
        - CDC (Change Data Capture) tracking
        - Query performance optimization
        - Partition pruning
        """
        # Cache the dataframe and count records before writing
        df = df.cache()
        record_count = df.count()

        if self.config.minio.use_minio:
            s3_path = self.config.get_s3_path("bronze", taxi_type=self.taxi_type)
            self.logger.info(f"Writing {record_count:,} records to MinIO bronze layer: {s3_path}")
            self.logger.info(f"Partitioning by: year={self.year}, month={self.month}")

            # Partition by year and month for optimal performance and delta operations
            df.write \
                .mode("append") \
                .partitionBy("year", "month") \
                .option("compression", "snappy") \
                .parquet(s3_path)

            self.logger.info(f"Successfully loaded {record_count:,} records to bronze layer")
            self.logger.info(f"Path: {s3_path}/year={self.year}/month={self.month}/")
        else:
            # If not using MinIO, save locally with partitioning
            output_path = f"{self.config.cache_dir}/output/{self.taxi_type}"
            self.logger.info(f"Writing {record_count:,} records to local path: {output_path}")
            df.write \
                .mode("append") \
                .partitionBy("year", "month") \
                .option("compression", "snappy") \
                .parquet(output_path)

        df.unpersist()


def run_ingestion(
    taxi_type: Literal["yellow", "green"],
    year: int,
    month: int
) -> bool:
    """
    Convenience function to run the ingestion job for a single month.

    Args:
        taxi_type: Type of taxi (yellow or green)
        year: Year of the data
        month: Month of the data

    Returns:
        True if successful, False otherwise
    """
    job = TaxiIngestionJob(taxi_type, year, month)
    return job.run()


def run_bulk_ingestion(
    taxi_type: Literal["yellow", "green"],
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int
) -> dict:
    """
    Run ingestion for multiple months (historical data ingestion).

    Args:
        taxi_type: Type of taxi (yellow or green)
        start_year: Starting year
        start_month: Starting month (1-12)
        end_year: Ending year
        end_month: Ending month (1-12)

    Returns:
        Dictionary with results for each month
    """
    from datetime import date
    from dateutil.relativedelta import relativedelta
    import logging

    logger = logging.getLogger(__name__)

    # Generate list of (year, month) tuples
    current_date = date(start_year, start_month, 1)
    end_date = date(end_year, end_month, 1)

    results = {}
    total_months = 0
    successful = 0
    failed = 0

    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        total_months += 1

        logger.info(f"Processing {taxi_type} taxi data for {year}-{month:02d} ({total_months})")

        try:
            success = run_ingestion(taxi_type, year, month)
            results[f"{year}-{month:02d}"] = "SUCCESS" if success else "FAILED"
            if success:
                successful += 1
            else:
                failed += 1
        except Exception as e:
            logger.error(f"Error processing {year}-{month:02d}: {e}")
            results[f"{year}-{month:02d}"] = f"ERROR: {str(e)}"
            failed += 1

        # Move to next month
        current_date += relativedelta(months=1)

    logger.info(f"Bulk ingestion complete: {successful}/{total_months} successful, {failed} failed")
    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC Taxi Data Ingestion Job")
    parser.add_argument("--taxi-type", type=str, choices=["yellow", "green"], required=True)
    parser.add_argument("--year", type=int, help="Year of the data (for single month ingestion)")
    parser.add_argument("--month", type=int, help="Month of the data (for single month ingestion)")
    parser.add_argument("--start-year", type=int, help="Start year (for bulk ingestion)")
    parser.add_argument("--start-month", type=int, help="Start month (for bulk ingestion)")
    parser.add_argument("--end-year", type=int, help="End year (for bulk ingestion)")
    parser.add_argument("--end-month", type=int, help="End month (for bulk ingestion)")

    args = parser.parse_args()

    # Determine if single or bulk ingestion
    if args.year and args.month:
        # Single month ingestion
        success = run_ingestion(args.taxi_type, args.year, args.month)
        exit(0 if success else 1)
    elif args.start_year and args.start_month and args.end_year and args.end_month:
        # Bulk ingestion
        results = run_bulk_ingestion(
            args.taxi_type,
            args.start_year,
            args.start_month,
            args.end_year,
            args.end_month
        )
        # Print summary
        print("\n=== Bulk Ingestion Results ===")
        for period, result in results.items():
            print(f"{period}: {result}")

        # Exit with success if all succeeded
        failed_count = sum(1 for r in results.values() if r != "SUCCESS")
        exit(0 if failed_count == 0 else 1)
    else:
        parser.error("Either provide --year and --month for single ingestion, or --start-year, --start-month, --end-year, --end-month for bulk ingestion")
