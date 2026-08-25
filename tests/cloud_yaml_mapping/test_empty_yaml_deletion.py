from unittest.mock import Mock, patch

from dbt_jobs_as_code.cloud_yaml_mapping.change_set import ChangeSet, build_change_set
from dbt_jobs_as_code.schemas.common_types import Settings, Triggers
from dbt_jobs_as_code.schemas.job import JobDefinition

JOB_YAML_TEMPLATE = """
jobs:
  {identifier}:
    account_id: {account_id}
    project_id: {project_id}
    environment_id: {environment_id}
    name: "Managed Job [[{identifier}]]"
    settings:
      threads: 4
    execution:
      timeout_seconds: 0
    run_generate_sources: false
    execute_steps:
      - dbt run
    generate_docs: false
    schedule:
      cron: "0 * * * *"
    triggers:
      schedule: true
"""


def _cloud_job(identifier="managed-job-1", job_id=1, project_id=123, environment_id=456):
    """A single identified (managed) job that exists in dbt Cloud."""
    return JobDefinition(
        id=job_id,
        identifier=identifier,
        project_id=project_id,
        environment_id=environment_id,
        account_id=789,
        name=f"Managed Job [[{identifier}]]",
        settings=Settings(threads=4),
        run_generate_sources=False,
        execute_steps=["dbt run"],
        generate_docs=False,
        schedule={"cron": "0 * * * *"},
        triggers=Triggers(schedule=True),
    )


def _mock_dbt_cloud(mock_dbt_cloud_class, cloud_jobs):
    instance = mock_dbt_cloud_class.return_value
    instance.get_jobs.return_value = cloud_jobs
    instance.get_env_vars.return_value = {}
    instance.build_mapping_job_identifier_job_id.return_value = {}
    return instance


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
def test_empty_yaml_scoped_with_account_id_proposes_deletion(mock_dbt_cloud_class, tmp_path):
    """An emptied YAML (`jobs: {}`), scoped with project+env+account-id, should
    propose deleting the job that still exists in dbt Cloud (issue #152)."""
    config_file = tmp_path / "jobs.yml"
    config_file.write_text("jobs: {}\n")
    instance = _mock_dbt_cloud(mock_dbt_cloud_class, [_cloud_job()])

    change_set = build_change_set(
        config=str(config_file),
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[123],
        environment_ids=[456],
        account_id=789,
    )

    instance.get_jobs.assert_called_once()
    _, kwargs = mock_dbt_cloud_class.call_args
    assert kwargs.get("account_id") == 789

    deletes = [c for c in change_set if c.action == "delete" and c.type == "job"]
    assert len(deletes) == 1
    assert deletes[0].identifier == "managed-job-1"


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
def test_empty_yaml_across_multiple_files_proposes_deletion(mock_dbt_cloud_class, tmp_path):
    """When several files are matched by the config pattern and ALL of them declare
    an empty `jobs` key, the merged result is still treated as an intentionally
    emptied config and deletion reconciliation kicks in."""
    (tmp_path / "jobs_a.yml").write_text("jobs: {}\n")
    (tmp_path / "jobs_b.yml").write_text("jobs: []\n")
    instance = _mock_dbt_cloud(mock_dbt_cloud_class, [_cloud_job()])

    change_set = build_change_set(
        config=str(tmp_path / "*.yml"),
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[123],
        environment_ids=[456],
        account_id=789,
    )

    instance.get_jobs.assert_called_once()
    deletes = [c for c in change_set if c.action == "delete" and c.type == "job"]
    assert len(deletes) == 1


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
def test_directory_with_no_job_files_does_not_delete(mock_dbt_cloud_class, tmp_path):
    """A directory/glob pattern that matches zero job-bearing YAML (e.g. a stray
    non-job file) must NOT be treated as an intentionally emptied jobs config, even
    when -p/-e/--account-id are all provided - there's nothing here that declared
    `jobs` at all."""
    (tmp_path / "unrelated.yml").write_text("foo: bar\n")
    instance = _mock_dbt_cloud(mock_dbt_cloud_class, [_cloud_job()])

    change_set = build_change_set(
        config=str(tmp_path / "*.yml"),
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[123],
        environment_ids=[456],
        account_id=789,
    )

    instance.get_jobs.assert_not_called()
    assert isinstance(change_set, ChangeSet)
    assert len(change_set) == 0


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
def test_partial_empty_multiple_files_no_mass_delete(mock_dbt_cloud_class, tmp_path):
    """One file in the matched set is emptied but another still defines a job -
    the merged config is non-empty, so this is the normal update/create/delete path,
    not the "config declares no jobs" path. Any job dropped from the emptied file is
    still reconciled normally via the regular deleted_jobs diff."""
    (tmp_path / "jobs_a.yml").write_text("jobs: {}\n")
    (tmp_path / "jobs_b.yml").write_text(
        JOB_YAML_TEMPLATE.format(
            identifier="kept-job", account_id=789, project_id=123, environment_id=456
        )
    )
    # dbt Cloud still has the job managed by jobs_b.yml, and one that used to live in
    # jobs_a.yml (now removed) - only the latter should be proposed for deletion.
    instance = _mock_dbt_cloud(
        mock_dbt_cloud_class,
        [_cloud_job(identifier="kept-job"), _cloud_job(identifier="removed-job")],
    )

    change_set = build_change_set(
        config=str(tmp_path / "*.yml"),
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
    )

    instance.get_jobs.assert_called_once()
    deletes = [c for c in change_set if c.action == "delete" and c.type == "job"]
    assert len(deletes) == 1
    assert deletes[0].identifier == "removed-job"


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
def test_empty_yaml_unscoped_bails_out(mock_dbt_cloud_class, tmp_path):
    """An emptied YAML with no project/env/account-id scoping must NOT propose
    deletions (avoids an accidental account-wide delete)."""
    config_file = tmp_path / "jobs.yml"
    config_file.write_text("jobs: {}\n")
    instance = _mock_dbt_cloud(mock_dbt_cloud_class, [_cloud_job()])

    change_set = build_change_set(
        config=str(config_file),
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
    )

    instance.get_jobs.assert_not_called()
    assert len(change_set) == 0


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
def test_empty_yaml_scoped_without_account_id_bails_out(mock_dbt_cloud_class, tmp_path):
    """Project/env scoping alone isn't enough for an emptied YAML - there's no job
    left to infer an account_id from, so --account-id must be provided explicitly."""
    config_file = tmp_path / "jobs.yml"
    config_file.write_text("jobs: {}\n")
    instance = _mock_dbt_cloud(mock_dbt_cloud_class, [_cloud_job()])

    change_set = build_change_set(
        config=str(config_file),
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[123],
        environment_ids=[456],
    )

    instance.get_jobs.assert_not_called()
    assert len(change_set) == 0


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
def test_limit_projects_envs_to_yml_with_empty_config_bails(mock_dbt_cloud_class, tmp_path):
    """--limit-projects-envs-to-yml derives its scope from the YAML's own jobs, so an
    emptied config can never produce a project/environment scope to reconcile
    against, even if --account-id is passed."""
    config_file = tmp_path / "jobs.yml"
    config_file.write_text("jobs: {}\n")
    instance = _mock_dbt_cloud(mock_dbt_cloud_class, [_cloud_job()])

    change_set = build_change_set(
        config=str(config_file),
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
        limit_projects_envs_to_yml=True,
        account_id=789,
    )

    instance.get_jobs.assert_not_called()
    assert len(change_set) == 0


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
def test_non_empty_yaml_filtered_to_zero_scoped_to_both_proposes_deletion(
    mock_dbt_cloud_class, tmp_path
):
    """When the YAML defines jobs but none match the requested project/environment
    scope, and the run is scoped to both --project-id and --environment-id, this is
    a deliberate "delete every job in this project+environment" request - the
    account_id can be sourced from the job(s) defined elsewhere in the YAML, no
    --account-id needed."""
    config_file = tmp_path / "jobs.yml"
    config_file.write_text(
        JOB_YAML_TEMPLATE.format(
            identifier="other-job", account_id=789, project_id=999, environment_id=888
        )
    )
    instance = _mock_dbt_cloud(mock_dbt_cloud_class, [_cloud_job()])

    change_set = build_change_set(
        config=str(config_file),
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[123],
        environment_ids=[456],
    )

    instance.get_jobs.assert_called_once()
    _, kwargs = mock_dbt_cloud_class.call_args
    assert kwargs.get("account_id") == 789
    deletes = [c for c in change_set if c.action == "delete" and c.type == "job"]
    assert len(deletes) == 1
    assert deletes[0].identifier == "managed-job-1"


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
def test_non_empty_yaml_filtered_to_zero_scoped_to_one_dimension_bails_out(
    mock_dbt_cloud_class, tmp_path
):
    """Scoping by only --project-id (or only --environment-id) isn't enough to
    treat "filtered to zero" as a deletion signal - it stays a no-op, same as
    today's behavior, to avoid wiping out every job in a whole project or
    environment."""
    config_file = tmp_path / "jobs.yml"
    config_file.write_text(
        JOB_YAML_TEMPLATE.format(
            identifier="other-job", account_id=789, project_id=999, environment_id=888
        )
    )
    instance = _mock_dbt_cloud(mock_dbt_cloud_class, [_cloud_job()])

    change_set = build_change_set(
        config=str(config_file),
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[123],
        environment_ids=[],
    )

    instance.get_jobs.assert_not_called()
    assert len(change_set) == 0


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.glob.glob")
def test_account_id_override_does_not_affect_non_empty_path(
    mock_glob, mock_dbt_cloud_class, mock_load_config
):
    """--account-id must only matter for the emptied-config case. When the YAML
    defines jobs, the account_id used to talk to dbt Cloud must still come from the
    job definitions themselves, never from an --account-id override."""
    mock_glob.return_value = ["jobs.yml"]
    job = _cloud_job(identifier="a-job")
    mock_config = Mock()
    mock_config.jobs = {"a-job": job}
    mock_load_config.return_value = mock_config
    instance = _mock_dbt_cloud(mock_dbt_cloud_class, [])

    build_change_set(
        config="jobs.yml",
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
        account_id=999999,
    )

    _, kwargs = mock_dbt_cloud_class.call_args
    assert kwargs.get("account_id") == job.account_id
    assert instance.get_jobs.called
