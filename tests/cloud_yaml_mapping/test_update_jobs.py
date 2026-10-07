from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner
from ruamel.yaml import YAML

from dbt_jobs_as_code.client import DBTCloudException
from dbt_jobs_as_code.cloud_yaml_mapping.update_jobs import (
    UpdateJobsError,
    UpdateJobsOptions,
    build_update_change_set,
)
from dbt_jobs_as_code.main import cli
from dbt_jobs_as_code.schemas.common_types import Settings, Triggers
from dbt_jobs_as_code.schemas.job import JobDefinition

DBT_CLOUD_PATH = "dbt_jobs_as_code.cloud_yaml_mapping.update_jobs.DBTCloud"


def _cloud_job(job_id: int, identifier: str | None = None, **overrides) -> JobDefinition:
    defaults = dict(
        id=job_id,
        identifier=identifier,
        project_id=123,
        environment_id=456,
        account_id=789,
        name=f"Job {job_id}",
        settings=Settings(threads=4),
        run_generate_sources=False,
        execute_steps=["dbt run"],
        generate_docs=False,
        schedule={"cron": "0 * * * *"},
        triggers=Triggers(schedule=True),
    )
    return JobDefinition(**{**defaults, **overrides})


def _write_yaml(path, jobs: dict[str, JobDefinition], **per_job_edits) -> str:
    """Write jobs the way `import-jobs --include-linked-id` does, then apply edits per key."""
    content = {"jobs": {}}
    for key, job in jobs.items():
        job_dict = job.to_load_format(include_linked_id=True)
        job_dict.update(per_job_edits.get(key, {}))
        content["jobs"][key] = job_dict
    file = path / "jobs.yml"
    yaml = YAML(typ="safe")
    with open(file, "w") as f:
        yaml.dump(content, f)
    return str(file)


def _options(
    config: str, project_ids: list[int] | None = None, environment_ids: list[int] | None = None
) -> UpdateJobsOptions:
    return UpdateJobsOptions(
        config=config,
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=project_ids or [],
        environment_ids=environment_ids or [],
    )


@pytest.fixture
def mock_dbt_cloud():
    with patch(DBT_CLOUD_PATH) as mock_class:
        instance = mock_class.return_value
        instance.get_jobs.return_value = []
        yield instance


def test_unchanged_export_has_no_changes(tmp_path, mock_dbt_cloud):
    cloud_jobs = {"import_1": _cloud_job(1), "import_2": _cloud_job(2)}
    mock_dbt_cloud.get_jobs.return_value = list(cloud_jobs.values())
    config = _write_yaml(tmp_path, cloud_jobs)

    change_set = build_update_change_set(_options(config))

    assert len(change_set) == 0


def test_changed_field_creates_an_update_for_that_job_only(tmp_path, mock_dbt_cloud):
    cloud_jobs = {"import_1": _cloud_job(1), "import_2": _cloud_job(2)}
    mock_dbt_cloud.get_jobs.return_value = list(cloud_jobs.values())
    config = _write_yaml(
        tmp_path, cloud_jobs, import_2={"cost_optimization_features": ["dbt_state"]}
    )

    change_set = build_update_change_set(_options(config))

    assert len(change_set) == 1
    change = change_set.root[0]
    assert change.action == "update"
    assert change.identifier == "import_2"
    assert change.sync_function == mock_dbt_cloud.update_job
    job = change.parameters["job"]
    assert job.id == 2
    assert job.cost_optimization_features == ["dbt_state"]
    assert "cost_optimization_features" in str(change.differences)


def test_unlinked_job_keeps_no_identifier(tmp_path, mock_dbt_cloud):
    """The YAML key (import_1) is just a label: it must never become the job's identifier."""
    cloud_jobs = {"import_1": _cloud_job(1)}
    mock_dbt_cloud.get_jobs.return_value = list(cloud_jobs.values())
    config = _write_yaml(tmp_path, cloud_jobs, import_1={"generate_docs": True})

    change_set = build_update_change_set(_options(config))

    job = change_set.root[0].parameters["job"]
    assert job.identifier is None
    assert "[[" not in job.to_payload()


def test_managed_job_keeps_its_identifier(tmp_path, mock_dbt_cloud):
    cloud_jobs = {"my-job": _cloud_job(1, identifier="my-job")}
    mock_dbt_cloud.get_jobs.return_value = list(cloud_jobs.values())
    config = _write_yaml(tmp_path, cloud_jobs, **{"my-job": {"generate_docs": True}})

    change_set = build_update_change_set(_options(config))

    job = change_set.root[0].parameters["job"]
    assert job.identifier == "my-job"
    assert "[[my-job]]" in job.to_payload()


def test_managed_job_under_another_key_is_rejected(tmp_path, mock_dbt_cloud):
    mock_dbt_cloud.get_jobs.return_value = [_cloud_job(1, identifier="my-job")]
    config = _write_yaml(tmp_path, {"import_1": _cloud_job(1)})

    with pytest.raises(UpdateJobsError) as exc_info:
        build_update_change_set(_options(config))

    assert "managed by `sync` under the identifier 'my-job'" in str(exc_info.value)


def test_missing_linked_id_is_rejected(tmp_path, mock_dbt_cloud):
    config = _write_yaml(tmp_path, {"import_1": _cloud_job(1)}, import_1={"linked_id": None})

    with pytest.raises(UpdateJobsError) as exc_info:
        build_update_change_set(_options(config))

    assert "has no `linked_id`" in str(exc_info.value)
    mock_dbt_cloud.get_jobs.assert_not_called()


def test_duplicate_linked_id_is_rejected(tmp_path, mock_dbt_cloud):
    config = _write_yaml(
        tmp_path,
        {"import_1": _cloud_job(1), "import_2": _cloud_job(1)},
    )

    with pytest.raises(UpdateJobsError) as exc_info:
        build_update_change_set(_options(config))

    assert "`linked_id` 1 is used by several jobs: import_1, import_2" in str(exc_info.value)


def test_unknown_linked_id_is_rejected_and_all_errors_are_reported(tmp_path, mock_dbt_cloud):
    mock_dbt_cloud.get_jobs.return_value = [_cloud_job(1, identifier="other")]
    config = _write_yaml(tmp_path, {"import_1": _cloud_job(1), "import_2": _cloud_job(2)})

    with pytest.raises(UpdateJobsError) as exc_info:
        build_update_change_set(_options(config))

    assert len(exc_info.value.errors) == 2
    assert "`linked_id` 2 doesn't exist in dbt Cloud" in exc_info.value.errors[1]


def test_jobs_from_different_accounts_are_rejected(tmp_path, mock_dbt_cloud):
    config = _write_yaml(
        tmp_path,
        {"import_1": _cloud_job(1), "import_2": _cloud_job(2, account_id=999)},
    )

    with pytest.raises(UpdateJobsError) as exc_info:
        build_update_change_set(_options(config))

    assert "different accounts" in str(exc_info.value)


def test_filters_restrict_the_yaml_jobs_and_what_is_fetched(tmp_path, mock_dbt_cloud):
    cloud_jobs = {
        "import_1": _cloud_job(1),
        "import_2": _cloud_job(2, environment_id=457),
    }
    mock_dbt_cloud.get_jobs.return_value = [cloud_jobs["import_2"]]
    config = _write_yaml(
        tmp_path,
        cloud_jobs,
        import_1={"generate_docs": True},
        import_2={"generate_docs": True},
    )

    change_set = build_update_change_set(_options(config, environment_ids=[457]))

    assert [change.identifier for change in change_set] == ["import_2"]
    mock_dbt_cloud.get_jobs.assert_called_once_with(project_ids=[123], environment_ids=[457])


def test_jobs_in_dbt_cloud_but_not_in_the_yaml_are_left_alone(tmp_path, mock_dbt_cloud):
    mock_dbt_cloud.get_jobs.return_value = [_cloud_job(1), _cloud_job(2)]
    config = _write_yaml(tmp_path, {"import_1": _cloud_job(1)})

    change_set = build_update_change_set(_options(config))

    assert len(change_set) == 0


def test_self_deferring_job_is_not_a_false_diff(tmp_path, mock_dbt_cloud):
    """dbt Cloud returns deferring_job_definition_id == the job's own id; the export
    turns that into `self_deferring: true`, which must compare as identical."""
    cloud_job = _cloud_job(1, deferring_job_definition_id=1)
    mock_dbt_cloud.get_jobs.return_value = [cloud_job]
    config = _write_yaml(tmp_path, {"import_1": cloud_job})

    change_set = build_update_change_set(_options(config))

    assert len(change_set) == 0


def test_env_vars_in_the_yaml_are_not_updated_and_trigger_a_warning(tmp_path, mock_dbt_cloud):
    cloud_job = _cloud_job(1)
    mock_dbt_cloud.get_jobs.return_value = [cloud_job]
    config = _write_yaml(
        tmp_path,
        {"import_1": cloud_job},
        import_1={"custom_environment_variables": [{"DBT_FOO": "bar"}]},
    )

    with patch("dbt_jobs_as_code.cloud_yaml_mapping.update_jobs.logger") as mock_logger:
        change_set = build_update_change_set(_options(config))

    assert len(change_set) == 0
    assert "custom_environment_variables" in mock_logger.warning.call_args.args[0]


def test_empty_selection_does_nothing(tmp_path, mock_dbt_cloud):
    config = _write_yaml(tmp_path, {"import_1": _cloud_job(1)})

    change_set = build_update_change_set(_options(config, project_ids=[1]))

    assert len(change_set) == 0
    mock_dbt_cloud.get_jobs.assert_not_called()


# ============= CLI =============


@pytest.fixture
def cli_flow(tmp_path, mock_dbt_cloud):
    cloud_jobs = {"import_1": _cloud_job(1), "import_2": _cloud_job(2)}
    mock_dbt_cloud.get_jobs.return_value = list(cloud_jobs.values())
    config = _write_yaml(
        tmp_path, cloud_jobs, import_1={"generate_docs": True}, import_2={"generate_docs": True}
    )
    return config, mock_dbt_cloud


def test_cli_dry_run_does_not_update(cli_flow):
    config, mock_dbt_cloud = cli_flow

    result = CliRunner().invoke(cli, ["update-jobs", config, "--dry-run"])

    assert result.exit_code == 0, result.output
    mock_dbt_cloud.update_job.assert_not_called()


def test_cli_updates_the_changed_jobs(cli_flow):
    config, mock_dbt_cloud = cli_flow

    result = CliRunner().invoke(cli, ["update-jobs", config])

    assert result.exit_code == 0, result.output
    assert [call.kwargs["job"].id for call in mock_dbt_cloud.update_job.call_args_list] == [1, 2]


def test_cli_exits_1_and_updates_nothing_when_a_job_cant_be_matched(cli_flow):
    config, mock_dbt_cloud = cli_flow
    mock_dbt_cloud.get_jobs.return_value = [_cloud_job(1)]

    result = CliRunner().invoke(cli, ["update-jobs", config])

    assert result.exit_code == 1
    mock_dbt_cloud.update_job.assert_not_called()


def test_cli_exits_1_when_an_update_fails_but_tries_the_others(cli_flow):
    config, mock_dbt_cloud = cli_flow
    mock_dbt_cloud.update_job.side_effect = [DBTCloudException("boom"), Mock()]

    result = CliRunner().invoke(cli, ["update-jobs", config])

    assert result.exit_code == 1
    assert mock_dbt_cloud.update_job.call_count == 2


def test_cli_fail_fast_stops_at_the_first_failure(cli_flow):
    config, mock_dbt_cloud = cli_flow
    mock_dbt_cloud.update_job.side_effect = DBTCloudException("boom")

    result = CliRunner().invoke(cli, ["update-jobs", config, "--fail-fast"])

    assert result.exit_code == 1
    assert mock_dbt_cloud.update_job.call_count == 1


def test_cli_reports_no_files_found(tmp_path):
    result = CliRunner().invoke(cli, ["update-jobs", str(tmp_path / "nope.yml")])

    assert result.exit_code == 1
