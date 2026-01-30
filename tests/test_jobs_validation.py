import pytest

from etl.jobs.bronze.taxi_ingestion_job import TaxiIngestionJob
from etl.jobs.gold.taxi_gold_job import TaxiGoldJob
from etl.jobs.load.postgres_load_job import PostgresLoadJob


def test_taxi_ingestion_validate_parameters_valid():
    TaxiIngestionJob._validate_parameters("yellow", 2024, 1)


def test_taxi_ingestion_validate_parameters_invalid():
    with pytest.raises(ValueError):
        TaxiIngestionJob._validate_parameters("blue", 2024, 1)

    with pytest.raises(ValueError):
        TaxiIngestionJob._validate_parameters("yellow", 2000, 1)

    with pytest.raises(ValueError):
        TaxiIngestionJob._validate_parameters("yellow", 2024, 13)


def test_taxi_gold_validate_parameters_valid():
    TaxiGoldJob._validate_parameters("green", 2023, 6, 2023, 7)


def test_taxi_gold_validate_parameters_invalid():
    with pytest.raises(ValueError):
        TaxiGoldJob._validate_parameters("blue", 2023, 1, None, None)

    with pytest.raises(ValueError):
        TaxiGoldJob._validate_parameters("yellow", 2000, 1, None, None)

    with pytest.raises(ValueError):
        TaxiGoldJob._validate_parameters("yellow", 2023, 13, None, None)

    with pytest.raises(ValueError):
        TaxiGoldJob._validate_parameters("yellow", 2023, 5, 2023, 4)


def test_postgres_load_invalid_taxi_type():
    with pytest.raises(ValueError):
        PostgresLoadJob("blue")
