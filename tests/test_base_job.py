import pytest

from etl.jobs.base_job import BaseSparkJob, JobExecutionError
from etl.jobs.utils.spark_manager import SparkSessionManager


class _DummySpark:
    pass


class _DummyJob(BaseSparkJob):
    def validate_inputs(self):
        self.validated = True

    def extract(self):
        return {"value": 1}

    def transform(self, df):
        return df

    def load(self, df):
        self.loaded = df


class _FailingExtractJob(BaseSparkJob):
    def validate_inputs(self):
        pass

    def extract(self):
        return None

    def transform(self, df):
        return df

    def load(self, df):
        pass


def test_base_job_run_success(monkeypatch):
    monkeypatch.setattr(
        SparkSessionManager,
        "get_session",
        lambda app_name, enable_s3: _DummySpark()
    )

    job = _DummyJob("dummy_job")
    assert job.run() is True
    assert job._metrics["status"] == "SUCCESS"
    assert "total_duration_seconds" in job._metrics
    assert isinstance(job.spark, _DummySpark)


def test_base_job_run_extract_none(monkeypatch):
    monkeypatch.setattr(
        SparkSessionManager,
        "get_session",
        lambda app_name, enable_s3: _DummySpark()
    )

    job = _FailingExtractJob("failing_extract")
    with pytest.raises(JobExecutionError):
        job.run()
