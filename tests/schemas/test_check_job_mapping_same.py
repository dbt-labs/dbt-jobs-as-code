from dbt_jobs_as_code.schemas import check_job_mapping_same
from dbt_jobs_as_code.schemas.job import JobDefinition


def test_check_job_mapping_same():
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
        environment_id=400,
        account_id=300,
        deferring_environment_id=400,
        settings={},
        run_generate_sources=False,
        execute_steps=[],
        generate_docs=False,
        schedule={"cron": "0 14 * * 0,1,2,3,4,5,6"},
        triggers={},
    )

    # Test that the jobs are different
    same, diff = check_job_mapping_same(mock_job1, mock_job2)
    assert not same
    assert diff is not None
    assert diff["status"] == "different"
