import os
from unittest.mock import Mock, patch

import pytest

from dbt_jobs_as_code.cloud_yaml_mapping.change_set import build_change_set
from dbt_jobs_as_code.schemas.common_types import Settings, Triggers
from dbt_jobs_as_code.schemas.config import Config
from dbt_jobs_as_code.schemas.job import JobDefinition


def _cloud_job(identifier="managed-job-1", job_id=1):
    """A single identified (managed) job that exists in dbt Cloud."""
    return JobDefinition(
        id=job_id,
        identifier=identifier,
        project_id=123,
        environment_id=456,
        account_id=789,
        name=f"Managed Job [[{identifier}]]",
        settings=Settings(threads=4),
        run_generate_sources=False,
        execute_steps=["dbt run"],
        generate_docs=False,
        schedule={"cron": "0 * * * *"},
        triggers=Triggers(schedule=True),
    )


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
def test_empty_yaml_scoped_proposes_deletion(mock_load, mock_dbt_cloud, tmp_path, monkeypatch):
    """An emptied YAML (jobs: {}), scoped with project+env and DBT_ACCOUNT_ID set,
    should propose deleting the job that still exists in dbt Cloud (issue #152)."""
    # YAML loads to zero jobs (the user removed the last job)
    mock_load.return_value = Config(jobs={})

    # dbt Cloud still has one managed job
    instance = mock_dbt_cloud.return_value
    instance.get_jobs.return_value = [_cloud_job()]
    instance.get_env_vars.return_value = {}
    instance.build_mapping_job_identifier_job_id.return_value = {}

    monkeypatch.setenv("DBT_ACCOUNT_ID", "789")

    # a real file must exist for the glob in build_change_set
    config_file = tmp_path / "jobs.yml"
    config_file.write_text("jobs: {}\n")

    change_set = build_change_set(
        config=str(config_file),
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[123],
        environment_ids=[456],
    )

    # get_jobs must actually be called (regression: it was skipped by the early return)
    instance.get_jobs.assert_called_once()

    deletes = [c for c in change_set if c.action == "delete" and c.type == "job"]
    assert len(deletes) == 1
    assert deletes[0].identifier == "managed-job-1"


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
def test_empty_yaml_unscoped_bails_out(mock_load, mock_dbt_cloud, tmp_path, monkeypatch):
    """An emptied YAML with no project/env scoping must NOT propose deletions
    (avoids an accidental account-wide delete)."""
    mock_load.return_value = Config(jobs={})

    instance = mock_dbt_cloud.return_value
    instance.get_jobs.return_value = [_cloud_job()]
    instance.get_env_vars.return_value = {}

    monkeypatch.delenv("DBT_ACCOUNT_ID", raising=False)

    config_file = tmp_path / "jobs.yml"
    config_file.write_text("jobs: {}\n")

    change_set = build_change_set(
        config=str(config_file),
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
    )

    instance.get_jobs.assert_not_called()
    assert len(change_set) == 0