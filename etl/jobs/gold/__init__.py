"""
Gold Layer ETL Jobs - Dimensional Modeling

This package contains PySpark jobs for the gold (curated) layer, implementing
a dimensional model (star schema) for analytics and reporting.
"""
try:
    from .taxi_gold_job import TaxiGoldJob, run_gold_job
except ImportError:
    # Fallback for absolute imports
    from etl.jobs.gold.taxi_gold_job import TaxiGoldJob, run_gold_job

__all__ = ["TaxiGoldJob", "run_gold_job"]
