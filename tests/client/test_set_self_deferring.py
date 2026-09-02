from unittest.mock import MagicMock

import pytest

from dbt_jobs_as_code.client import DBTCloud, DBTCloudException
from dbt_jobs_as_code.schemas.job import JobDefinition


def _make_job(**overrides):
    defaults = dict(
        name="Job",
        project_id=100,
        environment_id=200,
        account_id=300,
        settings={},
        run_generate_sources=False,
        execute_steps=[],
        generate_docs=False,
        schedule={"cron": "0 0 * * *"},
        triggers={},
        self_deferring=True,
        identifier="my-job",
    )
    return JobDefinition(**{**defaults, **overrides})


def test_set_self_deferring_resolves_id_and_patches_job():
    """A job just created in this run has no id yet at build time. set_self_deferring
    resolves its real id by identifier (the job now exists in dbt Cloud), then updates
    it to defer to itself."""
    client = DBTCloud(account_id=1, api_key="test")
    client.build_mapping_job_identifier_job_id = MagicMock(return_value={"my-job": 42})
    client.update_job = MagicMock(side_effect=lambda job: job)

    job = _make_job()
    result = client.set_self_deferring(job=job, identifier="my-job")

    client.build_mapping_job_identifier_job_id.assert_called_once()
    assert job.id == 42
    client.update_job.assert_called_once_with(job)
    assert result is job


def test_set_self_deferring_uses_own_id_in_payload():
    """The patched job's payload must self-reference the resolved id."""
    client = DBTCloud(account_id=1, api_key="test")
    client.build_mapping_job_identifier_job_id = MagicMock(return_value={"my-job": 99})
    client.update_job = MagicMock(side_effect=lambda job: job)

    job = _make_job()
    client.set_self_deferring(job=job, identifier="my-job")

    import json

    payload = json.loads(job.to_payload())
    assert payload["deferring_job_definition_id"] == 99


def test_set_self_deferring_raises_dbt_cloud_exception_when_job_was_never_created():
    """If the preceding create Change failed, the job's identifier won't be in the
    mapping. This must surface as a DBTCloudException (caught by ChangeSet.apply()'s
    per-change error handling), not an unhandled KeyError that would abort the whole
    apply() loop -- skipping every other queued change."""
    client = DBTCloud(account_id=1, api_key="test")
    client.build_mapping_job_identifier_job_id = MagicMock(return_value={})
    client.update_job = MagicMock()

    job = _make_job()
    with pytest.raises(DBTCloudException):
        client.set_self_deferring(job=job, identifier="my-job")

    client.update_job.assert_not_called()
