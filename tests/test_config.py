import pytest

import etl.jobs.utils.config as config_module
from etl.jobs.utils.config import JobConfig


def test_job_config_singleton_and_reset(tmp_path, monkeypatch):
    JobConfig.reset()
    monkeypatch.setattr(config_module.tempfile, "gettempdir", lambda: str(tmp_path))

    cfg1 = JobConfig()
    cfg2 = JobConfig()

    assert cfg1 is cfg2
    assert cfg1.cache_dir.exists()

    JobConfig.reset()
    cfg3 = JobConfig()
    assert cfg3 is not cfg1


def test_job_config_get_s3_path_valid():
    JobConfig.reset()
    cfg = JobConfig()

    bronze_path = cfg.get_s3_path("bronze", taxi_type="yellow")
    assert bronze_path.endswith("/bronze/nyc_taxi/yellow")

    gold_path = cfg.get_s3_path("gold")
    assert gold_path.endswith("/gold/nyc_taxi")


def test_job_config_get_s3_path_invalid():
    JobConfig.reset()
    cfg = JobConfig()

    with pytest.raises(ValueError):
        cfg.get_s3_path("platinum")
