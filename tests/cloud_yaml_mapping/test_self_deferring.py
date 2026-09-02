from unittest.mock import Mock, patch

from dbt_jobs_as_code.cloud_yaml_mapping.change_set import (
    BuildChangeSetOptions,
    build_change_set,
)
from dbt_jobs_as_code.schemas.common_types import Settings, Triggers
from dbt_jobs_as_code.schemas.job import JobDefinition


def _make_job(identifier: str, **overrides):
    defaults = dict(
        project_id=123,
        environment_id=456,
        account_id=789,
        name="Job",
        settings=Settings(threads=4),
        run_generate_sources=False,
        execute_steps=["dbt run"],
        generate_docs=False,
        schedule={"cron": "0 * * * *"},
        triggers=Triggers(schedule=True),
        identifier=identifier,
    )
    return JobDefinition(**{**defaults, **overrides})


def _mock_dbt_cloud(mock_dbt_cloud_class):
    mock_dbt_cloud = Mock()
    mock_dbt_cloud_class.return_value = mock_dbt_cloud
    mock_dbt_cloud.get_jobs.return_value = []
    mock_dbt_cloud.build_mapping_job_identifier_job_id.return_value = {}
    return mock_dbt_cloud


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.glob.glob")
def test_new_self_deferring_job_queues_a_follow_up_change(
    mock_glob, mock_dbt_cloud_class, mock_load_config
):
    """A brand-new self-deferring job can't self-reference on create (no id yet), so
    it must get a follow-up Change patching it to defer to itself once created."""
    job = _make_job("new-job", self_deferring=True)
    mock_config = Mock()
    mock_config.jobs = {"new-job": job}
    mock_load_config.return_value = mock_config
    mock_glob.return_value = ["test.yml"]
    mock_dbt_cloud = _mock_dbt_cloud(mock_dbt_cloud_class)

    change_set = build_change_set(
        BuildChangeSetOptions(
            config="test.yml",
            yml_vars=None,
            disable_ssl_verification=False,
            project_ids=[],
            environment_ids=[],
        )
    )

    job_changes = [c for c in change_set if c.type == "job"]
    assert len(job_changes) == 2

    create_change = next(c for c in job_changes if c.action == "create")
    assert create_change.identifier == "new-job"
    assert create_change.sync_function == mock_dbt_cloud.create_job

    patch_change = next(c for c in job_changes if c.action == "update")
    assert patch_change.identifier == "new-job:self-defer"
    assert patch_change.sync_function == mock_dbt_cloud.set_self_deferring
    assert patch_change.parameters == {"job": job, "identifier": "new-job"}


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.glob.glob")
def test_new_non_self_deferring_job_has_no_follow_up_change(
    mock_glob, mock_dbt_cloud_class, mock_load_config
):
    """Jobs that don't use self_deferring get only the normal create Change."""
    job = _make_job("plain-job")
    mock_config = Mock()
    mock_config.jobs = {"plain-job": job}
    mock_load_config.return_value = mock_config
    mock_glob.return_value = ["test.yml"]
    _mock_dbt_cloud(mock_dbt_cloud_class)

    change_set = build_change_set(
        BuildChangeSetOptions(
            config="test.yml",
            yml_vars=None,
            disable_ssl_verification=False,
            project_ids=[],
            environment_ids=[],
        )
    )

    job_changes = [c for c in change_set if c.type == "job"]
    assert len(job_changes) == 1
    assert job_changes[0].action == "create"


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.glob.glob")
def test_existing_job_turning_on_self_deferring_needs_no_follow_up_change(
    mock_glob, mock_dbt_cloud_class, mock_load_config
):
    """An existing job's cloud id is already known at diff time (change_set.py copies
    it onto the YAML job before returning), so turning on self_deferring for a job
    that already exists must self-reference in a single update Change -- no follow-up
    patch needed, unlike brand-new jobs."""
    yaml_job = _make_job("existing-job", self_deferring=True)
    cloud_job = _make_job("existing-job", id=42, deferring_job_definition_id=None)

    mock_config = Mock()
    mock_config.jobs = {"existing-job": yaml_job}
    mock_load_config.return_value = mock_config
    mock_glob.return_value = ["test.yml"]
    mock_dbt_cloud = Mock()
    mock_dbt_cloud_class.return_value = mock_dbt_cloud
    mock_dbt_cloud.get_jobs.return_value = [cloud_job]
    mock_dbt_cloud.build_mapping_job_identifier_job_id.return_value = {"existing-job": 42}
    mock_dbt_cloud.get_env_vars.return_value = {}

    change_set = build_change_set(
        BuildChangeSetOptions(
            config="test.yml",
            yml_vars=None,
            disable_ssl_verification=False,
            project_ids=[],
            environment_ids=[],
        )
    )

    job_changes = [c for c in change_set if c.type == "job"]
    assert len(job_changes) == 1
    update_change = job_changes[0]
    assert update_change.action == "update"
    assert update_change.identifier == "existing-job"
    assert update_change.sync_function == mock_dbt_cloud.update_job

    patched_job = update_change.parameters["job"]
    assert patched_job.id == 42
    import json

    payload = json.loads(patched_job.to_payload())
    assert payload["deferring_job_definition_id"] == 42
