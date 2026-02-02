"""Unit tests for taxi_ingestion_dag.py"""

from datetime import datetime, timedelta

import pytest


class TestTaxiIngestionDag:
    """Tests for the taxi ingestion DAG."""

    @pytest.fixture
    def dag(self):
        """Load the DAG for testing."""
        from dags.taxi_ingestion_dag import dag

        return dag

    def test_dag_id(self, dag):
        """Test DAG has correct ID."""
        assert dag.dag_id == "taxi_ingestion_dag"

    def test_dag_description(self, dag):
        """Test DAG has correct description."""
        assert dag.description == "Ingest taxi trip data from NYC TLC to Bronze layer"

    def test_dag_schedule(self, dag):
        """Test DAG has no schedule (manual trigger only)."""
        assert dag.schedule is None

    def test_dag_start_date(self, dag):
        """Test DAG start date."""
        assert dag.start_date == datetime(2024, 1, 1)

    def test_dag_catchup_disabled(self, dag):
        """Test catchup is disabled."""
        assert dag.catchup is False

    def test_dag_tags(self, dag):
        """Test DAG has correct tags."""
        assert set(dag.tags) == {"bronze", "ingestion", "taxi"}

    def test_dag_default_args_owner(self, dag):
        """Test default args owner."""
        assert dag.default_args["owner"] == "airflow"

    def test_dag_default_args_depends_on_past(self, dag):
        """Test depends_on_past is False."""
        assert dag.default_args["depends_on_past"] is False

    def test_dag_default_args_retries(self, dag):
        """Test retries configuration."""
        assert dag.default_args["retries"] == 1

    def test_dag_default_args_retry_delay(self, dag):
        """Test retry delay configuration."""
        assert dag.default_args["retry_delay"] == timedelta(minutes=5)

    def test_dag_default_args_email_on_failure(self, dag):
        """Test email_on_failure is disabled."""
        assert dag.default_args["email_on_failure"] is False

    def test_dag_default_args_email_on_retry(self, dag):
        """Test email_on_retry is disabled."""
        assert dag.default_args["email_on_retry"] is False

    def test_dag_has_params(self, dag):
        """Test DAG has required parameters."""
        params = dag.params
        assert "taxi_type" in params
        assert "start_year" in params
        assert "start_month" in params
        assert "end_year" in params
        assert "end_month" in params

    def test_dag_param_defaults(self, dag):
        """Test DAG parameter default values."""
        params = dag.params
        assert params["taxi_type"].value == "yellow"
        assert params["start_year"].value == "2025"
        assert params["start_month"].value == "1"
        assert params["end_year"].value == "2025"
        assert params["end_month"].value == "11"

    def test_dag_has_ingest_task(self, dag):
        """Test DAG has the ingest_taxi_data task."""
        task_ids = [task.task_id for task in dag.tasks]
        assert "ingest_taxi_data" in task_ids

    def test_dag_task_count(self, dag):
        """Test DAG has exactly one task."""
        assert len(dag.tasks) == 1

    def test_ingest_task_exists(self, dag):
        """Test ingest task exists and has required attributes."""
        task = dag.get_task("ingest_taxi_data")
        assert task is not None
        assert hasattr(task, "bash_command")

    def test_ingest_task_bash_command_contains_module(self, dag):
        """Test bash command references correct module."""
        task = dag.get_task("ingest_taxi_data")
        assert "etl.jobs.bronze.taxi_ingestion_job" in task.bash_command

    def test_ingest_task_bash_command_contains_params(self, dag):
        """Test bash command contains all parameter placeholders."""
        task = dag.get_task("ingest_taxi_data")
        assert "{{ params.taxi_type }}" in task.bash_command
        assert "{{ params.start_year }}" in task.bash_command
        assert "{{ params.start_month }}" in task.bash_command
        assert "{{ params.end_year }}" in task.bash_command
        assert "{{ params.end_month }}" in task.bash_command
