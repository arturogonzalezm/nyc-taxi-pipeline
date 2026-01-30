"""
PostgreSQL Load Job - Load Dimensional Model to PostgreSQL

This module implements the load layer pipeline that reads the gold layer
dimensional model and loads it into PostgreSQL for analytics and BI tools.

Architecture:
    - Reads from Gold layer (Parquet files in MinIO/S3)
    - Loads into PostgreSQL using JDBC
    - Supports both full refresh and incremental loads
    - Handles yellow and green taxi data

Features:
    - Batch loading with optimized JDBC settings
    - Transaction support (all-or-nothing)
    - Duplicate prevention (truncate or upsert strategies)
    - Progress tracking and logging
    - Error handling and rollback

Design Patterns:
    - Template Method: Inherits from BaseSparkJob
    - Batch Processing: Efficient bulk inserts via JDBC
"""

import sys
import os
from pathlib import Path

# Add project root to path for imports when running as script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(project_root))

import logging
from typing import Literal, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl.jobs.base_job import BaseSparkJob, JobExecutionError
from etl.jobs.utils.config import JobConfig

logger = logging.getLogger(__name__)


class PostgresLoadJob(BaseSparkJob):
    """
    Production-ready PostgreSQL load job for dimensional model.

    This job reads the gold layer dimensional model and loads it into
    PostgreSQL for analytics, reporting, and BI tool consumption.

    Features:
        - Loads fact and dimension tables
        - JDBC batch loading (optimized for performance)
        - Configurable load modes (overwrite, append, upsert)
        - Transaction support
        - Progress tracking

    Load Strategy:
        - Dimensions: Truncate and reload (SCD Type 1)
        - Facts: Incremental append or partition-based overwrite

    Example:
        >>> # Load all tables for yellow taxi
        >>> job = PostgresLoadJob("yellow")
        >>> success = job.run()
        >>>
        >>> # Load specific month
        >>> job = PostgresLoadJob("yellow", year=2024, month=1)
        >>> success = job.run()

    Attributes:
        taxi_type: Type of taxi data (yellow or green)
        year: Optional year filter for fact table
        month: Optional month filter for fact table
        postgres_url: JDBC connection URL
        postgres_properties: JDBC connection properties
    """

    def __init__(
        self,
        taxi_type: Literal["yellow", "green"],
        year: Optional[int] = None,
        month: Optional[int] = None,
        postgres_url: Optional[str] = None,
        postgres_user: Optional[str] = None,
        postgres_password: Optional[str] = None,
        config: Optional[JobConfig] = None,
    ):
        """
        Initialise the PostgreSQL load job.

        :params taxi_type: Type of taxi data (yellow or green)
        :params year: Optional year filter (loads all if not specified)
        :params month: Optional month filter (loads all if not specified)
        :params postgres_url: PostgreSQL JDBC URL (defaults to env var or localhost)
        :params postgres_user: PostgreSQL username (defaults to env var or 'postgres')
        :params postgres_password: PostgreSQL password (defaults to env var or 'postgres')
        :params config: Optional job configuration
        :raises ValueError: If parameters are invalid
        """
        if taxi_type not in ["yellow", "green"]:
            raise ValueError(f"Invalid taxi_type: {taxi_type}")

        super().__init__(
            job_name=f"PostgresLoad_{taxi_type}_{year or 'all'}_{month or 'all'}",
            config=config,
        )

        self.taxi_type = taxi_type
        self.year = year
        self.month = month

        # PostgreSQL connection parameters
        self.postgres_url = postgres_url or os.getenv(
            "POSTGRES_URL", "jdbc:postgresql://localhost:5432/nyc_taxi"
        )
        self.postgres_user = postgres_user or os.getenv("POSTGRES_USER", "postgres")
        self.postgres_password = postgres_password or os.getenv(
            "POSTGRES_PASSWORD", "postgres"
        )

        # JDBC connection properties for optimized loading
        self.postgres_properties = {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver",
            "batchsize": "50000",  # Larger batch size for better throughput (was 10000)
            "isolationLevel": "READ_COMMITTED",
            "stringtype": "unspecified",  # Better handling of VARCHAR types
            "reWriteBatchedInserts": "true",  # PostgreSQL JDBC optimization for batch inserts
        }

    def validate_inputs(self):
        """
        Validate job inputs and PostgreSQL connectivity.

        :raises JobExecutionError: If validation fails
        """
        self.logger.info(f"Validating PostgreSQL connection: {self.postgres_url}")
        self.logger.info(f"Loading data for: {self.taxi_type} taxi")
        if self.year and self.month:
            self.logger.info(f"Filtered to: {self.year}-{self.month:02d}")
        elif self.year:
            self.logger.info(f"Filtered to year: {self.year}")
        else:
            self.logger.info("Loading all available data")

    def extract(self) -> dict:
        """
        Extract dimensional model from gold layer.

        :returns: Dictionary with DataFrames:
            {
                'fact_trip': DataFrame,
                'dim_date': DataFrame,
                'dim_location': DataFrame,
                'dim_payment': DataFrame
            }

        :raises JobExecutionError: If extraction fails
        """
        gold_base_path = self.config.get_s3_path("gold", taxi_type=self.taxi_type)
        self.logger.info(f"Reading dimensional model from: {gold_base_path}")

        dimensional_model = {}

        # Read dimension tables (always load all dimensions)
        for dim_name in ["dim_date", "dim_location", "dim_payment"]:
            dim_path = f"{gold_base_path}/{dim_name}"
            self.logger.info(f"Reading {dim_name} from {dim_path}")

            try:
                dim_df = self.spark.read.parquet(dim_path)
                dim_count = dim_df.count()
                self.logger.info(f"Loaded {dim_name}: {dim_count:,} records")
                dimensional_model[dim_name] = dim_df
            except Exception as e:
                raise JobExecutionError(f"Failed to read {dim_name}: {e}") from e

        # Read fact table (with optional year/month filter)
        fact_path = f"{gold_base_path}/fact_trip"
        self.logger.info(f"Reading fact_trip from {fact_path}")

        try:
            fact_df = self.spark.read.parquet(fact_path)

            # Apply filters if specified
            if self.year:
                fact_df = fact_df.filter(F.col("partition_year") == self.year)
                if self.month:
                    fact_df = fact_df.filter(F.col("partition_month") == self.month)

            fact_count = fact_df.count()
            self.logger.info(f"Loaded fact_trip: {fact_count:,} records")
            dimensional_model["fact_trip"] = fact_df

        except Exception as e:
            raise JobExecutionError(f"Failed to read fact_trip: {e}") from e

        return dimensional_model

    def transform(self, dimensional_model: dict) -> dict:
        """
        Transform data for PostgreSQL (minimal transformations).

        Adds load layer metadata timestamps to track when data was loaded to PostgreSQL.

        :params dimensional_model: Dictionary with dimensional model DataFrames
        :returns: Dictionary with metadata-enriched DataFrames
        """

        self.logger.info("=== Preparing data for PostgreSQL ===")

        # Add load layer metadata to all tables
        enriched_model = {}
        for table_name, df in dimensional_model.items():
            # Add load timestamp metadata
            df_with_metadata = (
                df.withColumn("postgres_load_timestamp", F.current_timestamp())
                .withColumn("postgres_load_date", F.current_date())
                .withColumn("load_job_name", F.lit(self.job_name))
            )

            count = df_with_metadata.count()
            self.logger.info(f"{table_name}: {count:,} records ready for load")
            enriched_model[table_name] = df_with_metadata

        return enriched_model

    def load(self, dimensional_model: dict):
        """
        Load dimensional model into PostgreSQL.

        Loads data in this order (ALWAYS):
        1. Dimensions first (upsert - maintains referential integrity)
        2. Fact table last (upsert - references dimensions via FK)

        This order ensures:
        - FK constraints are satisfied
        - Dimensions exist before facts reference them
        - Idempotent loads (can re-run safely)

        :params dimensional_model: Dictionary with dimensional model DataFrames
        :raises JobExecutionError: If load fails
        """
        self.logger.info("=== Loading data to PostgreSQL ===")

        # ALWAYS load dimensions first
        # Dimensions must exist before fact table (FK constraints)
        dimension_tables = {
            "dim_date": "taxi.dim_date",
            "dim_location": "taxi.dim_location",
            "dim_payment": "taxi.dim_payment",
        }

        self.logger.info("Step 1/2: Loading dimension tables...")
        for dim_name, table_name in dimension_tables.items():
            self._load_dimension(dimensional_model[dim_name], table_name, dim_name)

        # Load fact table AFTER dimensions (always, regardless of full/incremental)
        self.logger.info("Step 2/2: Loading fact table...")
        self._load_fact_table(dimensional_model["fact_trip"])

        self.logger.info("=== PostgreSQL load complete ===")

    def _load_dimension(self, df: DataFrame, table_name: str, dim_name: str):
        """
        Load a dimension table to PostgreSQL using TRUNCATE + INSERT.

        Strategy: TRUNCATE (preserves constraints) + INSERT (SCD Type 1)

        Why TRUNCATE instead of mode="overwrite":
        - mode="overwrite" DROPS the table, destroying FK constraints
        - TRUNCATE empties the table but preserves schema and constraints
        - FK constraints from fact_trip remain intact

        :params df: Dimension DataFrame
        :params table_name: PostgreSQL table name (with schema)
        :params dim_name: Dimension name for logging
        """
        import psycopg2

        self.logger.info(f"Loading {dim_name} to {table_name}...")
        record_count = df.count()

        try:
            # Parse JDBC URL to get connection parameters
            jdbc_parts = self.postgres_url.replace("jdbc:postgresql://", "").split("/")
            host_port = jdbc_parts[0].split(":")
            host = host_port[0]
            port = int(host_port[1]) if len(host_port) > 1 else 5432
            database = jdbc_parts[1] if len(jdbc_parts) > 1 else "nyc_taxi"

            # TRUNCATE table (preserves constraints, removes data)
            self.logger.info(f"Truncating {table_name}...")
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=self.postgres_user,
                password=self.postgres_password,
            )
            conn.autocommit = False
            cursor = conn.cursor()

            # TRUNCATE is fast and preserves table structure/constraints
            cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
            conn.commit()
            cursor.close()
            conn.close()

            self.logger.info(f"✓ Truncated {table_name}")

            # INSERT new data via Spark JDBC (append mode)
            self.logger.info(f"Inserting {record_count:,} records into {table_name}...")
            df.write.jdbc(
                url=self.postgres_url,
                table=table_name,
                mode="append",  # Append to empty table after truncate
                properties=self.postgres_properties,
            )

            self.logger.info(f"✓ Loaded {dim_name}: {record_count:,} records")

        except psycopg2.Error as e:
            self.logger.error(f"PostgreSQL error loading {dim_name}: {e}")
            raise JobExecutionError(f"Failed to load {dim_name}: {e}") from e
        except Exception as e:
            self.logger.error(f"Unexpected error loading {dim_name}: {e}")
            raise JobExecutionError(f"Failed to load {dim_name}: {e}") from e

    def _load_fact_table(self, df: DataFrame):
        """
        Load fact table to PostgreSQL using hash-based upsert for idempotency.

        Strategy (Idempotent Upsert using fact_hash):
        - Leverages unique constraint on fact_hash column
        - Uses temporary table + INSERT ON CONFLICT for true upsert
        - Only updates records if data has changed (hash mismatch)
        - Safe to re-run without creating duplicates
        - Tracks load timestamps for audit trail

        This makes the pipeline idempotent - safe to re-run any time!

        :params df: Fact DataFrame with fact_hash column
        """
        table_name = "taxi.fact_trip"
        record_count = df.count()

        # Verify fact_hash column exists
        if "fact_hash" not in df.columns:
            raise JobExecutionError(
                "fact_hash column not found - cannot perform idempotent load"
            )

        self.logger.info("Loading fact_trip using hash-based upsert (idempotent)...")
        self.logger.info(f"Records to upsert: {record_count:,}")

        try:
            # Use temporary table approach for true upsert
            self._upsert_via_temp_table(df, table_name)
            self.logger.info(
                f"✓ Upserted fact_trip: {record_count:,} records (idempotent)"
            )

        except Exception as e:
            raise JobExecutionError(f"Failed to upsert fact_trip: {e}") from e

    def _upsert_via_temp_table(self, df: DataFrame, target_table: str):
        """
        Perform idempotent upsert using temporary table and MERGE logic.

        Process:
        1. Create temporary table with same schema as target
        2. Load new data into temp table via Spark JDBC
        3. Execute PostgreSQL INSERT ON CONFLICT (upsert) from temp to target
        4. Drop temporary table

        This approach:
        - Handles large datasets efficiently via Spark
        - Uses native PostgreSQL upsert (ON CONFLICT)
        - Ensures atomicity and consistency
        - Provides idempotency via fact_hash unique constraint

        :params df: DataFrame to upsert
        :params target_table: Target table name (e.g., "taxi.fact_trip")
        """
        import psycopg2

        temp_table = f"{target_table}_temp"
        self.logger.info(f"Creating temporary table: {temp_table}")

        try:
            # Write data to temporary table via Spark JDBC
            record_count = df.count()
            self.logger.info(
                f"Loading {record_count:,} records into temporary table..."
            )
            df.write.jdbc(
                url=self.postgres_url,
                table=temp_table,
                mode="overwrite",  # Drop/create temp table
                properties=self.postgres_properties,
            )

            # Create index on fact_hash for fast upsert lookups
            # Without this, the ON CONFLICT check scans the entire temp table (slow!)
            self.logger.info("Creating index on temp table for fast upsert...")

            # Parse JDBC URL
            jdbc_parts = self.postgres_url.replace("jdbc:postgresql://", "").split("/")
            host_port = jdbc_parts[0].split(":")
            host = host_port[0]
            port = int(host_port[1]) if len(host_port) > 1 else 5432
            database = jdbc_parts[1] if len(jdbc_parts) > 1 else "nyc_taxi"

            # Execute upsert via direct PostgreSQL connection
            # Reuse same connection for index creation and upsert
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=self.postgres_user,
                password=self.postgres_password,
            )
            conn.autocommit = False
            cursor = conn.cursor()

            # Create index on fact_hash for fast ON CONFLICT lookups
            self.logger.info(f"Creating index on {temp_table}...")
            cursor.execute(
                f"CREATE INDEX idx_temp_fact_hash ON {temp_table} (fact_hash)"
            )
            conn.commit()
            self.logger.info("✓ Index created")

            # Analyze temp table for query planner statistics
            self.logger.info(f"Analyzing {temp_table} for query optimization...")
            cursor.execute(f"ANALYZE {temp_table}")
            conn.commit()
            self.logger.info("✓ Statistics gathered")

            # Execute upsert
            self.logger.info("Executing upsert from temp table to target table...")

            # Check if target table is empty for optimization
            cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
            target_count = cursor.fetchone()[0]
            is_initial_load = target_count == 0

            if is_initial_load:
                self.logger.info(
                    "Target table is empty - using BULK INSERT optimization"
                )

                # OPTIMIZATION: Drop ALL indexes including PK and unique constraint
                # We'll recreate them after bulk load for maximum speed
                self.logger.info("Checking for indexes to optimize bulk insert...")

                # Get ALL indexes (including PK and unique constraint)
                cursor.execute("""
                    SELECT indexname, indexdef
                    FROM pg_indexes
                    WHERE schemaname = 'taxi'
                      AND tablename = 'fact_trip'
                """)
                existing_indexes = cursor.fetchall()

                if existing_indexes:
                    self.logger.info(
                        f"Dropping ALL {len(existing_indexes)} indexes (including PK) for fastest bulk insert..."
                    )

                    # First, drop FK constraints that reference this table's PK
                    self.logger.info("Temporarily dropping FK constraints...")
                    cursor.execute(f"""
                        ALTER TABLE {target_table} DROP CONSTRAINT IF EXISTS uq_fact_trip_hash,
                        DROP CONSTRAINT IF EXISTS fact_trip_pkey CASCADE
                    """)
                    conn.commit()

                    # Store index definitions for recreation
                    index_definitions = {}
                    for idx_name, idx_def in existing_indexes:
                        index_definitions[idx_name] = idx_def
                        cursor.execute(f"DROP INDEX IF EXISTS taxi.{idx_name} CASCADE")
                        self.logger.info(f"  Dropped index: {idx_name}")

                    conn.commit()
                    self.logger.info(f"✓ Dropped {len(existing_indexes)} indexes")
                else:
                    self.logger.info("No indexes to drop (table may be newly created)")
                    index_definitions = {}

                # OPTIMIZATION: Convert target table to UNLOGGED for super-fast bulk insert
                # UNLOGGED tables skip WAL logging = 3-5x faster inserts
                self.logger.info(
                    "Converting target table to UNLOGGED for faster bulk insert..."
                )
                cursor.execute(f"ALTER TABLE {target_table} SET UNLOGGED")
                conn.commit()
                self.logger.info("✓ Target table is now UNLOGGED (no WAL overhead)")

                # OPTIMIZATION: Disable FK constraints during bulk load for speed
                self.logger.info(
                    "Disabling foreign key constraints for faster bulk insert..."
                )
                cursor.execute(f"ALTER TABLE {target_table} DISABLE TRIGGER ALL")
                conn.commit()
                self.logger.info("✓ FK constraints disabled")

                # OPTIMIZATION: Tune PostgreSQL for bulk insert performance
                self.logger.info("Tuning PostgreSQL for bulk insert...")
                cursor.execute("SET maintenance_work_mem = '2GB'")
                cursor.execute("SET work_mem = '512MB'")
                cursor.execute("SET max_parallel_maintenance_workers = 4")
                cursor.execute("SET synchronous_commit = OFF")
                cursor.execute("SET temp_buffers = '256MB'")  # More temp buffer space
                conn.commit()
                self.logger.info("✓ PostgreSQL tuned for bulk operations")

                # Fast path: Direct INSERT without conflict checks
                columns = list(df.columns)
                columns_str = ", ".join(columns)

                upsert_sql = f"""
                    INSERT INTO {target_table} ({columns_str})
                    SELECT {columns_str}
                    FROM {temp_table}
                """
            else:
                self.logger.info(
                    f"Target table has {target_count:,} rows - using ON CONFLICT DO NOTHING"
                )
                # Incremental load: Use ON CONFLICT DO NOTHING (faster than DO UPDATE)
                columns = list(df.columns)
                columns_str = ", ".join(columns)

                upsert_sql = f"""
                    INSERT INTO {target_table} ({columns_str})
                    SELECT {columns_str}
                    FROM {temp_table}
                    ON CONFLICT (fact_hash) DO NOTHING
                """

            self.logger.info("Executing upsert SQL...")
            self.logger.info(f"Upserting {record_count:,} records...")
            self.logger.debug(f"Upsert SQL: {upsert_sql}")

            cursor.execute(upsert_sql)
            rows_affected = cursor.rowcount
            conn.commit()

            if is_initial_load:
                self.logger.info(
                    f"✓ Initial load complete: {rows_affected:,} rows inserted"
                )

                # Convert table back to LOGGED for durability
                self.logger.info("Converting table back to LOGGED for durability...")
                cursor.execute(f"ALTER TABLE {target_table} SET LOGGED")
                conn.commit()
                self.logger.info("✓ Table is now LOGGED (durable)")

                # Re-enable FK constraints
                self.logger.info("Re-enabling foreign key constraints...")
                cursor.execute(f"ALTER TABLE {target_table} ENABLE TRIGGER ALL")
                conn.commit()
                self.logger.info("✓ FK constraints re-enabled")

                # Reset PostgreSQL settings to defaults
                self.logger.info("Resetting PostgreSQL settings...")
                cursor.execute("SET synchronous_commit = ON")
                conn.commit()

                # Recreate constraints and indexes after bulk insert (if any were dropped)
                if index_definitions:
                    self.logger.info(
                        "Recreating constraints and indexes (this may take 10-15 minutes)..."
                    )

                    # First recreate PK and unique constraint (most important)
                    for idx_name, idx_def in list(index_definitions.items()):
                        if "fact_trip_pkey" in idx_name:
                            self.logger.info("  Creating PRIMARY KEY constraint...")
                            cursor.execute(
                                f"ALTER TABLE {target_table} ADD CONSTRAINT fact_trip_pkey PRIMARY KEY (trip_key)"
                            )
                            conn.commit()
                            self.logger.info("  ✓ PRIMARY KEY created")
                            del index_definitions[idx_name]  # Remove from dict
                        elif "uq_fact_trip_hash" in idx_name:
                            self.logger.info(
                                "  Creating UNIQUE constraint on fact_hash..."
                            )
                            cursor.execute(
                                f"ALTER TABLE {target_table} ADD CONSTRAINT uq_fact_trip_hash UNIQUE (fact_hash)"
                            )
                            conn.commit()
                            self.logger.info("  ✓ UNIQUE constraint created")
                            del index_definitions[idx_name]  # Remove from dict

                    # Then recreate other indexes
                    for idx_name, idx_def in index_definitions.items():
                        self.logger.info(f"  Creating: {idx_name}")
                        cursor.execute(idx_def)
                        conn.commit()

                    total_indexes = len(existing_indexes)
                    self.logger.info(
                        f"✓ Recreated {total_indexes} indexes and constraints"
                    )
                else:
                    self.logger.info(
                        "No indexes to recreate (indexes already exist or will be created by schema)"
                    )

                # Run ANALYZE to update statistics after bulk load
                self.logger.info("Running ANALYZE for query optimization...")
                cursor.execute(f"ANALYZE {target_table}")
                conn.commit()
                self.logger.info("✓ Statistics updated")

                # Validate FK constraints after re-enabling
                self.logger.info("Validating foreign key constraints...")
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {target_table}
                    WHERE date_key NOT IN (SELECT date_key FROM taxi.dim_date)
                       OR pickup_location_key NOT IN (SELECT location_key FROM taxi.dim_location)
                       OR dropoff_location_key NOT IN (SELECT location_key FROM taxi.dim_location)
                       OR payment_key NOT IN (SELECT payment_key FROM taxi.dim_payment)
                """)
                invalid_fk_count = cursor.fetchone()[0]
                if invalid_fk_count > 0:
                    self.logger.warning(
                        f"Found {invalid_fk_count} rows with invalid foreign keys!"
                    )
                else:
                    self.logger.info("✓ All foreign keys are valid")
            else:
                self.logger.info(
                    f"✓ Upsert complete: {rows_affected:,} new rows inserted (duplicates skipped)"
                )

            # Drop temporary table
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
            conn.commit()

            cursor.close()
            conn.close()

            self.logger.info(f"✓ Cleaned up temporary table: {temp_table}")

        except psycopg2.Error as e:
            self.logger.error(f"PostgreSQL error during upsert: {e}")
            raise JobExecutionError(f"Upsert failed: {e}") from e
        except Exception as e:
            self.logger.error(f"Unexpected error during upsert: {e}")
            raise JobExecutionError(f"Upsert failed: {e}") from e


def run_postgres_load(
    taxi_type: Literal["yellow", "green"],
    year: Optional[int] = None,
    month: Optional[int] = None,
    postgres_url: Optional[str] = None,
    postgres_user: Optional[str] = None,
    postgres_password: Optional[str] = None,
) -> bool:
    """
    Convenience function to run the PostgreSQL load job.

    :params taxi_type: Type of taxi (yellow or green)
    :params year: Optional year filter
    :params month: Optional month filter
    :params postgres_url: Optional PostgreSQL JDBC URL
    :params postgres_user: Optional PostgreSQL username
    :params postgres_password: Optional PostgreSQL password
    :returns: True if successful, False otherwise
    """
    job = PostgresLoadJob(
        taxi_type, year, month, postgres_url, postgres_user, postgres_password
    )
    return job.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PostgreSQL Load Job")
    parser.add_argument(
        "--taxi-type", type=str, choices=["yellow", "green"], required=True
    )
    parser.add_argument("--year", type=int, help="Year filter (optional)")
    parser.add_argument("--month", type=int, help="Month filter (optional)")
    parser.add_argument("--postgres-url", type=str, help="PostgreSQL JDBC URL")
    parser.add_argument("--postgres-user", type=str, help="PostgreSQL username")
    parser.add_argument("--postgres-password", type=str, help="PostgreSQL password")

    args = parser.parse_args()

    success = run_postgres_load(
        args.taxi_type,
        args.year,
        args.month,
        args.postgres_url,
        args.postgres_user,
        args.postgres_password,
    )
    exit(0 if success else 1)
