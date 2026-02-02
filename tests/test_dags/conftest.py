"""Conftest for DAG tests - mocks Airflow modules."""
import sys
from unittest.mock import MagicMock
from datetime import datetime, timedelta

# Create mock Airflow modules before any DAG imports
mock_dag = MagicMock()
mock_bash_operator = MagicMock()

# Mock the DAG class
class MockDAG:
    def __init__(self, dag_id, default_args=None, description=None, schedule=None,
                 start_date=None, catchup=False, tags=None, params=None):
        self.dag_id = dag_id
        self.default_args = default_args or {}
        self.description = description
        self.schedule = schedule
        self.start_date = start_date
        self.catchup = catchup
        self.tags = tags or []
        self.params = {}
        self._tasks = []
        
        # Convert params dict to mock Param objects with .value attribute
        if params:
            for key, value in params.items():
                param_mock = MagicMock()
                param_mock.value = value
                self.params[key] = param_mock
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        pass
    
    @property
    def tasks(self):
        return self._tasks
    
    def get_task(self, task_id):
        for task in self._tasks:
            if task.task_id == task_id:
                return task
        return None


class MockBashOperator:
    def __init__(self, task_id, bash_command, dag=None):
        self.task_id = task_id
        self.bash_command = bash_command
        self.dag = dag
        # Register with current DAG context
        if dag:
            dag._tasks.append(self)


# Store current DAG context for operators
_current_dag = None


class MockDAGContext(MockDAG):
    def __enter__(self):
        global _current_dag
        _current_dag = self
        return self
    
    def __exit__(self, *args):
        global _current_dag
        _current_dag = None


class MockBashOperatorWithContext:
    def __init__(self, task_id, bash_command, dag=None):
        global _current_dag
        self.task_id = task_id
        self.bash_command = bash_command
        target_dag = dag or _current_dag
        if target_dag:
            target_dag._tasks.append(self)


# Setup mock modules
airflow_mock = MagicMock()
airflow_mock.DAG = MockDAGContext

operators_mock = MagicMock()
operators_mock.bash = MagicMock()
operators_mock.bash.BashOperator = MockBashOperatorWithContext

sys.modules['airflow'] = airflow_mock
sys.modules['airflow.operators'] = operators_mock
sys.modules['airflow.operators.bash'] = operators_mock.bash

# Make DAG available from airflow module
airflow_mock.DAG = MockDAGContext
