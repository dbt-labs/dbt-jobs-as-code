from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from dbt_jobs_as_code.main import import_jobs
from dbt_jobs_as_code.schemas.common_types import Settings, Triggers
from dbt_jobs_as_code.schemas.job import JobDefinition


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
