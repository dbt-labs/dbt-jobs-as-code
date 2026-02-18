from unittest.mock import Mock

import pytest

from dbt_jobs_as_code.importer import fetch_jobs, get_account_id
from dbt_jobs_as_code.schemas.job import JobDefinition


def test_get_account_id():
    # Test account ID from direct input
    assert get_account_id(None, 123) == 123

    # Test missing both inputs
    with pytest.raises(ValueError):
        get_account_id(None, None)


def test_fetch_jobs():
    mock_dbt = Mock()

    # Mock job objects
    mock_job1 = JobDefinition(
        id=1,
        name="Job 1",
        project_id=100,
        environment_id=200,
        account_id=300,
        settings={},
        run_generate_sources=False,
        execute_steps=[],
        generate_docs=False,
        schedule={"cron": "0 14 * * 0,1,2,3,4,5,6"},
        triggers={},
    )
    mock_job2 = JobDefinition(
        id=2,
        name="Job 2",
        project_id=100,
        environment_id=200,
        account_id=300,
        settings={},
        run_generate_sources=False,
        execute_steps=[],
        generate_docs=False,
        schedule={"cron": "0 14 * * 0,1,2,3,4,5,6"},
        triggers={},
    )

    # Set return values for mocks
    mock_dbt.get_job.side_effect = [mock_job1, mock_job2]
    mock_dbt.get_jobs.return_value = [mock_job1, mock_job2]

    # Test fetch with only job IDs
    jobs = fetch_jobs(mock_dbt, [1, 2], [], [])
    assert mock_dbt.get_job.call_count == 2
    assert len(jobs) == 2

    # Test fetch with project IDs
    jobs = fetch_jobs(mock_dbt, [1], [100], [])
    mock_dbt.get_jobs.assert_called_with(project_ids=[100], environment_ids=[])
    assert len(jobs) == 1
