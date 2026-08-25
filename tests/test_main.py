import json
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from dbt_jobs_as_code.cloud_yaml_mapping.change_set import Change, ChangeSet
from dbt_jobs_as_code.main import cli, import_jobs
from dbt_jobs_as_code.schemas.common_types import Settings, Triggers
from dbt_jobs_as_code.schemas.job import JobDefinition

# ============= Fixtures =============


@pytest.fixture
def mock_dbt_cloud():
    with patch("dbt_jobs_as_code.main.DBTCloud") as mock:
        instance = mock.return_value
        # Create base job with common parameters
        base_job = JobDefinition(
            project_id=123,
            environment_id=456,
            account_id=789,
            name="Base Job",
            settings=Settings(threads=4),
            run_generate_sources=False,
            execute_steps=["dbt run"],
            generate_docs=False,
            schedule={"cron": "0 * * * *"},
            triggers=Triggers(schedule=True),
        )

        instance.get_jobs.return_value = [
            base_job.model_copy(
                update={
                    "id": 1,
                    "name": "Managed Job 1",
                    "identifier": "managed-job-1",
                    "triggers": Triggers(schedule=True, github_webhook=True),
                }
            ),
            base_job.model_copy(
                update={
                    "id": 2,
                    "name": "Managed Job 2",
                    "identifier": "managed-job-2",
                }
            ),
            base_job.model_copy(
                update={
                    "id": 3,
                    "name": "Unmanaged Job",
                    "identifier": None,
                }
            ),
        ]
        # Mock get_env_vars to return empty dict
        instance.get_env_vars.return_value = {}
        yield instance


@pytest.fixture
def mock_change_set():
    """Create a mock change set with both job and env var changes"""
    change_set = ChangeSet()

    base_job = JobDefinition(
        project_id=123,
        environment_id=456,
        account_id=789,
        name="job1",
        settings=Settings(threads=4),
        run_generate_sources=False,
        execute_steps=["dbt run"],
        generate_docs=False,
        schedule={"cron": "0 * * * *"},
        triggers=Triggers(schedule=True),
        identifier="job1",
        id=42,
    )

    # Add a job change
    change_set.append(
        Change(
            identifier="job1",
            type="job",
            action="update",
            proj_id=123,
            env_id=456,
            sync_function=Mock(return_value=base_job),
            parameters={},
            differences={
                "values_changed": {
                    "root['name']": {"new_value": "new_name", "old_value": "old_name"}
                }
            },
        )
    )

    # Add an env var change
    change_set.append(
        Change(
            identifier="job1:DBT_VAR1",
            type="env var overwrite",
            action="update",
            proj_id=123,
            env_id=456,
            sync_function=Mock(),
            parameters={},
            differences={"old_value": "old_val", "new_value": "new_val"},
        )
    )

    return change_set


@pytest.fixture
def mock_empty_change_set():
    """Create an empty change set"""
    return ChangeSet()


# ============= Import Command Tests =============


def test_import_jobs_managed_only(mock_dbt_cloud):
    """Test that --managed-only flag only imports jobs with identifiers"""
    runner = CliRunner()

    # Run with --managed-only flag
    result = runner.invoke(
        import_jobs,
        [
            "--account-id",
            "123",
            "--managed-only",
        ],
    )

    assert result.exit_code == 0

    # Check that managed jobs are in the output
    assert "managed-job-1" in result.stdout
    assert "managed-job-2" in result.stdout

    # Check that unmanaged job is not in the output
    assert "Unmanaged Job" not in result.stdout


def test_import_jobs_without_managed_only(mock_dbt_cloud):
    """Test that without --managed-only flag all jobs are imported"""
    runner = CliRunner()

    # Run without --managed-only flag
    result = runner.invoke(
        import_jobs,
        [
            "--account-id",
            "123",
        ],
    )

    assert result.exit_code == 0

    # Check that all jobs are in the output
    assert "managed-job-1" in result.stdout
    assert "managed-job-2" in result.stdout
    assert "Unmanaged Job" in result.stdout


# ============= Plan Command Tests =============


@patch("dbt_jobs_as_code.main.build_change_set")
def test_plan_command_json_output(mock_build_change_set, mock_change_set):
    """Test that plan command produces valid JSON output when --json flag is used"""
    mock_build_change_set.return_value = mock_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["plan", "--json", "config.yml"])

    assert result.exit_code == 0

    # Verify the output is valid JSON
    json_output = json.loads(result.output)

    # Verify structure
    assert "job_changes" in json_output
    assert "env_var_overwrite_changes" in json_output

    # Verify job changes
    assert len(json_output["job_changes"]) == 1
    job_change = json_output["job_changes"][0]
    assert job_change["identifier"] == "job1"
    assert job_change["action"] == "UPDATE"
    assert "differences" in job_change

    # Verify env var changes
    assert len(json_output["env_var_overwrite_changes"]) == 1
    env_var_change = json_output["env_var_overwrite_changes"][0]
    assert env_var_change["identifier"] == "job1:DBT_VAR1"
    assert env_var_change["action"] == "UPDATE"
    assert "differences" in env_var_change


@patch("dbt_jobs_as_code.main.build_change_set")
def test_plan_command_json_output_no_changes(mock_build_change_set, mock_empty_change_set):
    """Test that plan command produces valid JSON output with no changes"""
    mock_build_change_set.return_value = mock_empty_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["plan", "--json", "config.yml"])

    assert result.exit_code == 0

    # Verify the output is valid JSON
    json_output = json.loads(result.output)

    # Verify structure
    assert json_output == {
        "job_changes": [],
        "env_var_overwrite_changes": [],
    }


@patch("dbt_jobs_as_code.main.build_change_set")
def test_plan_command_regular_output(mock_build_change_set, mock_change_set):
    """Test that plan command produces regular output when --json flag is not used"""
    mock_build_change_set.return_value = mock_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["plan", "config.yml"])

    assert result.exit_code == 0

    # Verify this is not JSON
    with pytest.raises(json.JSONDecodeError):
        json.loads(result.output)


# ============= Sync Command Tests =============


@patch("dbt_jobs_as_code.main.build_change_set")
def test_sync_command_json_output(mock_build_change_set, mock_change_set):
    """Test that sync command produces valid JSON output when --json flag is used"""
    mock_build_change_set.return_value = mock_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["sync", "--json", "config.yml"])

    assert result.exit_code == 0

    # Verify the output is valid JSON
    json_output = json.loads(result.output)

    # Verify structure
    assert "job_changes" in json_output
    assert "env_var_overwrite_changes" in json_output
    assert "applied" in json_output
    assert "apply_success" in json_output

    # Verify job changes
    assert len(json_output["job_changes"]) == 1
    job_change = json_output["job_changes"][0]
    assert job_change["identifier"] == "job1"
    assert job_change["action"] == "UPDATE"
    assert "differences" in job_change

    # Verify env var changes
    assert len(json_output["env_var_overwrite_changes"]) == 1
    env_var_change = json_output["env_var_overwrite_changes"][0]
    assert env_var_change["identifier"] == "job1:DBT_VAR1"
    assert env_var_change["action"] == "UPDATE"
    assert "differences" in env_var_change

    # Verify applied results
    assert json_output["apply_success"] is True
    assert "job_changes" in json_output["applied"]
    assert "env_var_overwrite_changes" in json_output["applied"]

    applied_job = json_output["applied"]["job_changes"][0]
    assert applied_job["identifier"] == "job1"
    assert applied_job["action"] == "UPDATE"
    assert applied_job["job_id"] == 42


@patch("dbt_jobs_as_code.main.build_change_set")
def test_sync_command_json_output_no_changes(mock_build_change_set, mock_empty_change_set):
    """Test that sync command produces valid JSON output with no changes"""
    mock_build_change_set.return_value = mock_empty_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["sync", "--json", "config.yml"])

    assert result.exit_code == 0

    # Verify the output is valid JSON
    json_output = json.loads(result.output)

    # Verify structure
    assert json_output["job_changes"] == []
    assert json_output["env_var_overwrite_changes"] == []
    assert json_output["applied"] == {"job_changes": [], "env_var_overwrite_changes": []}
    assert json_output["apply_success"] is True


@patch("dbt_jobs_as_code.main.build_change_set")
def test_sync_command_regular_output(mock_build_change_set, mock_change_set):
    """Test that sync command produces regular output when --json flag is not used"""
    mock_build_change_set.return_value = mock_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["sync", "config.yml"])

    assert result.exit_code == 0

    # Verify this is not JSON
    with pytest.raises(json.JSONDecodeError):
        json.loads(result.output)


@patch("dbt_jobs_as_code.main.build_change_set")
def test_sync_command_with_fail_fast(mock_build_change_set):
    """Test that sync command passes fail_fast parameter to change_set.apply()"""
    mock_change_set = Mock()
    mock_change_set.__len__ = Mock(return_value=2)  # Non-empty change set
    mock_build_change_set.return_value = mock_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["sync", "--fail-fast", "config.yml"])

    assert result.exit_code == 0

    # Verify that apply was called with fail_fast=True
    mock_change_set.apply.assert_called_once_with(fail_fast=True)


@patch("dbt_jobs_as_code.main.build_change_set")
def test_sync_command_without_fail_fast(mock_build_change_set):
    """Test that sync command passes fail_fast=False by default to change_set.apply()"""
    mock_change_set = Mock()
    mock_change_set.__len__ = Mock(return_value=2)  # Non-empty change set
    mock_build_change_set.return_value = mock_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["sync", "config.yml"])

    assert result.exit_code == 0

    # Verify that apply was called with fail_fast=False (default)
    mock_change_set.apply.assert_called_once_with(fail_fast=False)


# ============= Exclude Identifiers Matching Tests =============


@patch("dbt_jobs_as_code.main.build_change_set")
def test_plan_command_with_exclude_identifiers_matching(
    mock_build_change_set, mock_empty_change_set
):
    """Test that plan command passes exclude_identifiers_matching parameter correctly"""
    mock_build_change_set.return_value = mock_empty_change_set

    runner = CliRunner()
    result = runner.invoke(
        cli, ["plan", "config.yml", "--exclude-identifiers-matching", "staging:.*"]
    )

    assert result.exit_code == 0

    # Verify that build_change_set was called with the correct exclude_identifiers_matching parameter
    mock_build_change_set.assert_called_once()
    options = mock_build_change_set.call_args[0][0]

    assert options.config == "config.yml"
    assert options.yml_vars is None
    assert options.disable_ssl_verification is False
    assert options.project_ids == []
    assert options.environment_ids == []
    assert options.limit_projects_envs_to_yml is False
    assert options.exclude_identifiers_matching == "staging:.*"


@patch("dbt_jobs_as_code.main.build_change_set")
def test_sync_command_with_exclude_identifiers_matching(
    mock_build_change_set, mock_empty_change_set
):
    """Test that sync command passes exclude_identifiers_matching parameter correctly"""
    mock_build_change_set.return_value = mock_empty_change_set

    runner = CliRunner()
    result = runner.invoke(
        cli, ["sync", "config.yml", "--exclude-identifiers-matching", "legacy:.*"]
    )

    assert result.exit_code == 0

    # Verify that build_change_set was called with the correct exclude_identifiers_matching parameter
    mock_build_change_set.assert_called_once()
    options = mock_build_change_set.call_args[0][0]

    assert options.exclude_identifiers_matching == "legacy:.*"


@patch("dbt_jobs_as_code.main.build_change_set")
def test_plan_command_without_exclude_identifiers_matching(
    mock_build_change_set, mock_empty_change_set
):
    """Test that plan command works when exclude_identifiers_matching is not provided"""
    mock_build_change_set.return_value = mock_empty_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["plan", "config.yml"])

    assert result.exit_code == 0

    # Verify that build_change_set was called with None for exclude_identifiers_matching
    mock_build_change_set.assert_called_once()
    options = mock_build_change_set.call_args[0][0]

    assert options.exclude_identifiers_matching is None


@patch("dbt_jobs_as_code.main.build_change_set")
def test_sync_command_without_exclude_identifiers_matching(
    mock_build_change_set, mock_empty_change_set
):
    """Test that sync command works when exclude_identifiers_matching is not provided"""
    mock_build_change_set.return_value = mock_empty_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["sync", "config.yml"])

    assert result.exit_code == 0

    # Verify that build_change_set was called with None for exclude_identifiers_matching
    mock_build_change_set.assert_called_once()
    options = mock_build_change_set.call_args[0][0]

    assert options.exclude_identifiers_matching is None


@patch("dbt_jobs_as_code.main.build_change_set")
def test_plan_command_with_complex_regex_pattern(mock_build_change_set, mock_empty_change_set):
    """Test that plan command handles complex regex patterns correctly"""
    mock_build_change_set.return_value = mock_empty_change_set

    runner = CliRunner()
    complex_pattern = "(staging|temp|legacy):.*test.*"
    result = runner.invoke(
        cli, ["plan", "config.yml", "--exclude-identifiers-matching", complex_pattern]
    )

    assert result.exit_code == 0

    # Verify that build_change_set was called with the correct complex pattern
    mock_build_change_set.assert_called_once()
    options = mock_build_change_set.call_args[0][0]

    assert options.exclude_identifiers_matching == complex_pattern


@patch("dbt_jobs_as_code.main.build_change_set")
def test_sync_command_with_json_and_exclude_pattern(mock_build_change_set, mock_empty_change_set):
    """Test that sync command works with both --json and --exclude-identifiers-matching flags"""
    mock_build_change_set.return_value = mock_empty_change_set

    runner = CliRunner()
    result = runner.invoke(
        cli, ["sync", "config.yml", "--json", "--exclude-identifiers-matching", "temp:.*"]
    )

    assert result.exit_code == 0

    # Verify that build_change_set was called with both parameters
    mock_build_change_set.assert_called_once()
    options = mock_build_change_set.call_args[0][0]

    assert options.exclude_identifiers_matching == "temp:.*"
    assert options.output_json is True


# ============= use_desc_for_id Option Tests =============


@patch("dbt_jobs_as_code.main.build_change_set")
def test_use_desc_for_id_option_sync(mock_build_change_set, mock_empty_change_set):
    """Test that sync command accepts --use-desc-for-id and passes it to build_change_set"""
    mock_build_change_set.return_value = mock_empty_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["sync", "--use-desc-for-id", "config.yml"])

    assert result.exit_code == 0

    mock_build_change_set.assert_called_once()
    options = mock_build_change_set.call_args[0][0]
    assert options.use_desc_for_id is True


@patch("dbt_jobs_as_code.main.build_change_set")
def test_use_desc_for_id_option_plan(mock_build_change_set, mock_empty_change_set):
    """Test that plan command accepts --use-desc-for-id and passes it to build_change_set"""
    mock_build_change_set.return_value = mock_empty_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["plan", "--use-desc-for-id", "config.yml"])

    assert result.exit_code == 0

    mock_build_change_set.assert_called_once()
    options = mock_build_change_set.call_args[0][0]
    assert options.use_desc_for_id is True


@patch("dbt_jobs_as_code.main.build_change_set")
def test_use_desc_for_id_default_false(mock_build_change_set, mock_empty_change_set):
    """Test that omitting --use-desc-for-id defaults to False"""
    mock_build_change_set.return_value = mock_empty_change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["plan", "config.yml"])

    assert result.exit_code == 0

    mock_build_change_set.assert_called_once()
    options = mock_build_change_set.call_args[0][0]
    assert options.use_desc_for_id is False


@patch("dbt_jobs_as_code.main.DBTCloud")
@patch("dbt_jobs_as_code.main.load_job_configuration")
@patch("dbt_jobs_as_code.main.resolve_file_paths")
def test_validate_online_empty_jobs_does_not_crash(
    mock_resolve_file_paths, mock_load_job_configuration, mock_DBTCloud
):
    """Regression test for issue #152: `validate --online` against a config that
    declares no jobs (`jobs: {}`) used to raise IndexError on
    `list(defined_jobs)[0].account_id` - it should now warn and skip the online
    check instead of crashing."""
    mock_resolve_file_paths.return_value = (["config.yml"], [])

    mock_config = Mock()
    mock_config.jobs = {}
    mock_load_job_configuration.return_value = mock_config

    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--online", "config.yml"])

    assert result.exit_code == 0
    mock_DBTCloud.assert_not_called()


@patch("dbt_jobs_as_code.main.DBTCloud")
@patch("dbt_jobs_as_code.main.load_job_configuration")
@patch("dbt_jobs_as_code.main.resolve_file_paths")
def test_use_desc_for_id_option_validate(
    mock_resolve_file_paths, mock_load_job_configuration, mock_DBTCloud
):
    """Test that validate --online passes use_desc_for_id=True to DBTCloud"""
    from dbt_jobs_as_code.schemas.common_types import Settings, Triggers
    from dbt_jobs_as_code.schemas.job import JobDefinition

    mock_resolve_file_paths.return_value = (["config.yml"], [])

    job = JobDefinition(
        project_id=123,
        environment_id=456,
        account_id=789,
        name="Test Job",
        settings=Settings(threads=4),
        run_generate_sources=False,
        execute_steps=["dbt run"],
        generate_docs=False,
        schedule={"cron": "0 * * * *"},
        triggers=Triggers(schedule=True),
    )
    mock_config = Mock()
    mock_config.jobs = {"test-job": job}
    mock_load_job_configuration.return_value = mock_config

    instance = mock_DBTCloud.return_value
    instance.get_environments.return_value = [{"id": 456, "project_id": 123}]
    instance.get_jobs.return_value = []

    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--online", "--use-desc-for-id", "config.yml"])

    assert result.exit_code == 0
    mock_DBTCloud.assert_called_once()
    assert mock_DBTCloud.call_args.kwargs["use_desc_for_id"] is True


@patch("dbt_jobs_as_code.main.DBTCloud")
def test_use_desc_for_id_option_import_jobs(mock_DBTCloud):
    """Test that import-jobs passes use_desc_for_id=True to DBTCloud"""
    instance = mock_DBTCloud.return_value
    instance.get_jobs.return_value = []
    instance.get_env_vars.return_value = {}

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["import-jobs", "--account-id", "789", "--use-desc-for-id"],
    )

    assert result.exit_code == 0
    mock_DBTCloud.assert_called_once()
    assert mock_DBTCloud.call_args.kwargs["use_desc_for_id"] is True


@patch("dbt_jobs_as_code.main.DBTCloud")
@patch("dbt_jobs_as_code.main.load_job_configuration")
@patch("dbt_jobs_as_code.main.resolve_file_paths")
def test_use_desc_for_id_option_link(
    mock_resolve_file_paths, mock_load_job_configuration, mock_DBTCloud
):
    """Test that link passes use_desc_for_id=True to DBTCloud"""
    from dbt_jobs_as_code.schemas.common_types import Settings, Triggers
    from dbt_jobs_as_code.schemas.job import JobDefinition

    mock_resolve_file_paths.return_value = (["config.yml"], [])

    job = JobDefinition(
        project_id=123,
        environment_id=456,
        account_id=789,
        name="Test Job",
        settings=Settings(threads=4),
        run_generate_sources=False,
        execute_steps=["dbt run"],
        generate_docs=False,
        schedule={"cron": "0 * * * *"},
        triggers=Triggers(schedule=True),
    )
    mock_config = Mock()
    mock_config.jobs = {"test-job": job}
    mock_load_job_configuration.return_value = mock_config

    runner = CliRunner()
    result = runner.invoke(cli, ["link", "--dry-run", "--use-desc-for-id", "config.yml"])

    assert result.exit_code == 0
    mock_DBTCloud.assert_called_once()
    assert mock_DBTCloud.call_args.kwargs["use_desc_for_id"] is True


@patch("dbt_jobs_as_code.main.DBTCloud")
def test_use_desc_for_id_option_unlink(mock_DBTCloud):
    """Test that unlink passes use_desc_for_id=True to DBTCloud"""
    instance = mock_DBTCloud.return_value
    instance.get_jobs.return_value = []

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["unlink", "--account-id", "789", "--use-desc-for-id"],
    )

    assert result.exit_code == 0
    mock_DBTCloud.assert_called_once()
    assert mock_DBTCloud.call_args.kwargs["use_desc_for_id"] is True


@patch("dbt_jobs_as_code.main.DBTCloud")
def test_use_desc_for_id_option_deactivate_jobs(mock_DBTCloud):
    """Test that deactivate-jobs passes use_desc_for_id=True to DBTCloud"""
    instance = mock_DBTCloud.return_value
    instance.get_jobs.return_value = []

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["deactivate-jobs", "--account-id", "789", "--use-desc-for-id"],
    )

    assert result.exit_code == 0
    mock_DBTCloud.assert_called_once()
    assert mock_DBTCloud.call_args.kwargs["use_desc_for_id"] is True
