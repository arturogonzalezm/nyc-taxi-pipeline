"""Unit tests for zone_lookup_ingestion_dag.py"""
from datetime import datetime, timedelta

import pytest


class TestZoneLookupIngestionDag:
    """Tests for the zone lookup ingestion DAG."""

    @pytest.fixture
    def dag(self):
        """Load the DAG for testing."""
        from dags.zone_lookup_ingestion_dag import dag
        return dag

    def test_dag_id(self, dag):
        """Test DAG has correct ID."""
        assert dag.dag_id == "zone_lookup_ingestion_dag"

    def test_dag_description(self, dag):
        """Test DAG has correct description."""
        assert dag.description == "Ingest taxi zone lookup reference data to Bronze layer"

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
        assert set(dag.tags) == {"bronze", "ingestion", "zone_lookup"}

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

    def test_dag_has_no_params(self, dag):
        """Test DAG has no custom parameters (zone lookup needs none)."""
        assert len(dag.params) == 0

    def test_dag_has_ingest_task(self, dag):
        """Test DAG has the ingest_zone_lookup task."""
        task_ids = [task.task_id for task in dag.tasks]
        assert "ingest_zone_lookup" in task_ids

    def test_dag_task_count(self, dag):
        """Test DAG has exactly one task."""
        assert len(dag.tasks) == 1

    def test_ingest_task_exists(self, dag):
        """Test ingest task exists and has required attributes."""
        task = dag.get_task("ingest_zone_lookup")
        assert task is not None
        assert hasattr(task, 'bash_command')

    def test_ingest_task_bash_command_contains_module(self, dag):
        """Test bash command references correct module."""
        task = dag.get_task("ingest_zone_lookup")
        assert "etl.jobs.bronze.zone_lookup_ingestion_job" in task.bash_command

    def test_ingest_task_bash_command_no_params(self, dag):
        """Test bash command has no parameter placeholders."""
        task = dag.get_task("ingest_zone_lookup")
        assert "{{ params" not in task.bash_command
