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

    # Add a job change (sync_function returns None so no result_job_id is captured)
    change_set.append(
        Change(
            identifier="job1",
            type="job",
            action="update",
            proj_id=123,
            env_id=456,
            sync_function=Mock(return_value=None),
            parameters={},
            differences={
                "values_changed": {
                    "root['name']": {"new_value": "new_name", "old_value": "old_name"}
                }
            },
        )
    )

    # Add an env var change (sync_function returns None so no result_job_id is captured)
    change_set.append(
        Change(
            identifier="job1:DBT_VAR1",
            type="env var overwrite",
            action="update",
            proj_id=123,
            env_id=456,
            sync_function=Mock(return_value=None),
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

    # Parse output - will have 2 JSON lines (planned changes and completed operations)
    # But completed operations will be empty since mocks return None
    output_lines = [line for line in result.output.strip().split("\n") if line]
    
    # First JSON output is the planned changes
    json_output = json.loads(output_lines[0])

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
    
    # Second JSON output is completed operations (will be empty since mocks return None)
    second_json = json.loads(output_lines[1])
    assert "completed_operations" in second_json
    assert len(second_json["completed_operations"]) == 0


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
    assert json_output == {
        "job_changes": [],
        "env_var_overwrite_changes": [],
    }


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
    call_args = mock_build_change_set.call_args

    # Check positional arguments
    assert call_args[0][0] == "config.yml"  # config
    assert call_args[0][1] is None  # vars_yml
    assert call_args[0][2] is False  # disable_ssl_verification
    assert call_args[0][3] == []  # project_ids
    assert call_args[0][4] == []  # environment_ids
    assert call_args[0][5] is False  # limit_projects_envs_to_yml
    assert call_args[0][6] == "staging:.*"  # exclude_identifiers_matching


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
    call_args = mock_build_change_set.call_args

    # Check that exclude_identifiers_matching parameter is passed correctly
    assert call_args[0][6] == "legacy:.*"  # exclude_identifiers_matching


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
    call_args = mock_build_change_set.call_args

    # Check that exclude_identifiers_matching parameter is None
    assert call_args[0][6] is None  # exclude_identifiers_matching


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
    call_args = mock_build_change_set.call_args

    # Check that exclude_identifiers_matching parameter is None
    assert call_args[0][6] is None  # exclude_identifiers_matching


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
    call_args = mock_build_change_set.call_args

    # Check that the complex pattern is passed correctly
    assert call_args[0][6] == complex_pattern  # exclude_identifiers_matching


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
    call_args = mock_build_change_set.call_args

    # Check that exclude_identifiers_matching parameter is passed
    assert call_args[0][6] == "temp:.*"  # exclude_identifiers_matching
    # Check that output_json is True
    assert call_args.kwargs.get("output_json") is True


# ============= Sync Command Completed Operations Tests =============


@patch("dbt_jobs_as_code.main.build_change_set")
def test_sync_command_outputs_completed_operations_json(mock_build_change_set):
    """Test that sync command outputs completed operations JSON after apply"""
    # Create a change set with operations that will have result_job_id
    change_set = ChangeSet()

    # Mock job creation
    job_mock = Mock()
    job_mock.id = 12345
    job_sync = Mock(return_value=job_mock)

    # Add a job change
    job_change = Change(
        identifier="test_job",
        type="job",
        action="create",
        proj_id=123,
        env_id=456,
        sync_function=job_sync,
        parameters={},
    )
    change_set.append(job_change)

    mock_build_change_set.return_value = change_set

    runner = CliRunner()
    result = runner.invoke(cli, ["sync", "--json", "config.yml"])

    assert result.exit_code == 0

    # Parse the output - should have two JSON objects (one before apply, one after)
    output_lines = [line for line in result.output.strip().split("\n") if line]
    assert len(output_lines) == 2

    # First JSON output is the planned changes
    first_json = json.loads(output_lines[0])
    assert "job_changes" in first_json
    assert len(first_json["job_changes"]) == 1

    # Second JSON output is the completed operations
    second_json = json.loads(output_lines[1])
    assert "completed_operations" in second_json
    assert len(second_json["completed_operations"]) == 1
    assert second_json["completed_operations"][0]["job_id"] == 12345
    assert second_json["completed_operations"][0]["identifier"] == "test_job"


@patch("dbt_jobs_as_code.main.build_change_set")
def test_sync_command_no_completed_operations_on_empty_changeset(mock_build_change_set):
    """Test that no completed operations JSON is output when changeset is empty"""
    mock_build_change_set.return_value = ChangeSet()

    runner = CliRunner()
    result = runner.invoke(cli, ["sync", "--json", "config.yml"])

    assert result.exit_code == 0

    # Should only have one JSON output (the empty changeset)
    output_lines = [line for line in result.output.strip().split("\n") if line]
    assert len(output_lines) == 1

    # Verify it's the empty changeset JSON
    json_output = json.loads(output_lines[0])
    assert json_output == {"job_changes": [], "env_var_overwrite_changes": []}
