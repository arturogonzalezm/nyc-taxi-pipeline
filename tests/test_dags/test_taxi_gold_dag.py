"""Unit tests for taxi_gold_dag.py"""
from datetime import datetime, timedelta

import pytest


class TestTaxiGoldDag:
    """Tests for the taxi gold DAG."""

    @pytest.fixture
    def dag(self):
        """Load the DAG for testing."""
        from dags.taxi_gold_dag import dag
        return dag

    def test_dag_id(self, dag):
        """Test DAG has correct ID."""
        assert dag.dag_id == "taxi_gold_dag"

    def test_dag_description(self, dag):
        """Test DAG has correct description."""
        assert dag.description == "Transform taxi data from Bronze to Gold layer (dimensional model)"

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
        assert set(dag.tags) == {"gold", "transformation", "taxi"}

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

    def test_dag_has_transform_task(self, dag):
        """Test DAG has the transform_to_gold task."""
        task_ids = [task.task_id for task in dag.tasks]
        assert "transform_to_gold" in task_ids

    def test_dag_task_count(self, dag):
        """Test DAG has exactly one task."""
        assert len(dag.tasks) == 1

    def test_transform_task_exists(self, dag):
        """Test transform task exists and has required attributes."""
        task = dag.get_task("transform_to_gold")
        assert task is not None
        assert hasattr(task, 'bash_command')

    def test_transform_task_bash_command_contains_module(self, dag):
        """Test bash command references correct module."""
        task = dag.get_task("transform_to_gold")
        assert "etl.jobs.gold.taxi_gold_job" in task.bash_command

    def test_transform_task_bash_command_contains_params(self, dag):
        """Test bash command contains all parameter placeholders."""
        task = dag.get_task("transform_to_gold")
        assert "{{ params.taxi_type }}" in task.bash_command
        assert "{{ params.start_year }}" in task.bash_command
        assert "{{ params.start_month }}" in task.bash_command
        assert "{{ params.end_year }}" in task.bash_command
        assert "{{ params.end_month }}" in task.bash_command
