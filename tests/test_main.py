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

    # Add a job change
    change_set.append(
        Change(
            identifier="job1",
            type="job",
            action="update",
            proj_id=123,
            env_id=456,
            sync_function=Mock(),
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
