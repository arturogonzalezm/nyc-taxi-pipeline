"""Unit tests for postgres_load_dag.py"""
from datetime import datetime, timedelta

import pytest


class TestPostgresLoadDag:
    """Tests for the postgres load DAG."""

    @pytest.fixture
    def dag(self):
        """Load the DAG for testing."""
        from dags.postgres_load_dag import dag
        return dag

    def test_dag_id(self, dag):
        """Test DAG has correct ID."""
        assert dag.dag_id == "postgres_load_dag"

    def test_dag_description(self, dag):
        """Test DAG has correct description."""
        assert dag.description == "Load taxi data from Gold layer to PostgreSQL"

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
        assert set(dag.tags) == {"load", "postgres", "taxi"}

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

    def test_dag_has_params(self, dag):
        """Test DAG has required parameters."""
        params = dag.params
        assert "taxi_type" in params

    def test_dag_param_defaults(self, dag):
        """Test DAG parameter default values."""
        params = dag.params
        assert params["taxi_type"].value == "yellow"

    def test_dag_has_load_task(self, dag):
        """Test DAG has the load_to_postgres task."""
        task_ids = [task.task_id for task in dag.tasks]
        assert "load_to_postgres" in task_ids

    def test_dag_task_count(self, dag):
        """Test DAG has exactly one task."""
        assert len(dag.tasks) == 1

    def test_load_task_exists(self, dag):
        """Test load task exists and has required attributes."""
        task = dag.get_task("load_to_postgres")
        assert task is not None
        assert hasattr(task, 'bash_command')

    def test_load_task_bash_command_contains_module(self, dag):
        """Test bash command references correct module."""
        task = dag.get_task("load_to_postgres")
        assert "etl.jobs.load.postgres_load_job" in task.bash_command

    def test_load_task_bash_command_contains_params(self, dag):
        """Test bash command contains taxi_type parameter placeholder."""
        task = dag.get_task("load_to_postgres")
        assert "{{ params.taxi_type }}" in task.bash_command
