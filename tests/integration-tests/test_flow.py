import io
import os
from pathlib import Path

import pytest
from click.testing import CliRunner
from loguru import logger

from dbt_jobs_as_code.client import DBTCloud
from dbt_jobs_as_code.main import cli

ACCOUNT_ID = int(os.getenv("DBT_ACCOUNT_ID", 0))
PROJECT_ID = int(os.getenv("DBT_PROJECT_ID", 0))
ENV_ID = int(os.getenv("DBT_ENV_ID", 0))
API_KEY = os.getenv("DBT_API_KEY")
BASE_URL = os.getenv("DBT_BASE_URL", "https://cloud.getdbt.com/")
JOB_ID = int(os.getenv("DBT_JOB_ID", 0))


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def log_capture():
    log_stream = io.StringIO()
    logger.remove()  # Remove any existing log handlers
    logger.add(log_stream, level="DEBUG")  # Add a new handler to capture logs in log_stream
    yield log_stream
    logger.remove()  # Clean up handler after test


def clean_up_jobs():
    dbt_client = DBTCloud(
        base_url=BASE_URL,
        api_key=API_KEY,
        account_id=ACCOUNT_ID,
    )
    all_jobs = dbt_client.get_jobs(project_ids=[PROJECT_ID], environment_ids=[ENV_ID])
    all_tracked_jobs = [job for job in all_jobs if job.identifier is not None]

    print()
    for job in all_tracked_jobs:
        print(f"Deleting the job {job.identifier} with id {job.id}")
        dbt_client.delete_job(job)


@pytest.mark.not_in_parallel
def test_end_to_end_flow_limit_project(runner, log_capture):
    if PROJECT_ID == 0:
        raise ValueError("Please set the DBT_CLOUD_PROJECT_ID environment variable")

    templated_job = Path("tests/integration-tests/jobs_template.yml").read_text()
    rendered_job_string = templated_job.format(
        project_id=PROJECT_ID,
        account_id=ACCOUNT_ID,
        environment_id=ENV_ID,
        job_id=JOB_ID,
    )

    # Create the relevant test file
    Path("tests/integration-tests/jobs_rendered.yml").write_text(rendered_job_string)

    try:
        # Call plan based on a file
        result_plan_1 = runner.invoke(
            cli, ["plan", "tests/integration-tests/jobs_rendered.yml", "-p", PROJECT_ID]
        )
        assert result_plan_1.exit_code == 0
        log_output = log_capture.getvalue()
        assert "-- PLAN --" in log_output
        assert "changes detected" in log_output
        assert "No changes detected" not in log_output

        log_capture.truncate(0)
        log_capture.seek(0)

        # Call sync based on a file
        result_sync_1 = runner.invoke(
            cli, ["sync", "tests/integration-tests/jobs_rendered.yml", "-p", PROJECT_ID]
        )
        assert result_sync_1.exit_code == 0
        log_output = log_capture.getvalue()
        assert "-- SYNC --" in log_output
        assert "| ERROR" not in log_output

        log_capture.truncate(0)
        log_capture.seek(0)

        # Call plan based on a file again
        result_plan_2 = runner.invoke(
            cli, ["plan", "tests/integration-tests/jobs_rendered.yml", "-p", PROJECT_ID]
        )
        assert result_plan_2.exit_code == 0
        log_output = log_capture.getvalue()
        assert "-- PLAN --" in log_output
        assert "No changes detected" in log_output

        log_capture.truncate(0)
        log_capture.seek(0)

        # Call sync based on a file again
        result_sync_2 = runner.invoke(
            cli, ["sync", "tests/integration-tests/jobs_rendered.yml", "-p", PROJECT_ID]
        )
        assert result_sync_2.exit_code == 0
        log_output = log_capture.getvalue()
        assert "-- SYNC --" in log_output
        assert "No changes detected" in log_output

    finally:
        # always clean, even on failure
        clean_up_jobs()


@pytest.mark.not_in_parallel
def test_end_to_end_flow_limit_yml(runner, log_capture):
    if PROJECT_ID == 0:
        raise ValueError("Please set the DBT_CLOUD_PROJECT_ID environment variable")

    templated_job = Path("tests/integration-tests/jobs_template.yml").read_text()
    rendered_job_string = templated_job.format(
        project_id=PROJECT_ID,
        account_id=ACCOUNT_ID,
        environment_id=ENV_ID,
        job_id=JOB_ID,
    )

    # Create the relevant test file
    Path("tests/integration-tests/jobs_rendered.yml").write_text(rendered_job_string)

    try:
        # Call plan based on a file
        result_plan_1 = runner.invoke(
            cli, ["plan", "tests/integration-tests/jobs_rendered.yml", "-l"]
        )
        assert result_plan_1.exit_code == 0
        log_output = log_capture.getvalue()
        assert "-- PLAN --" in log_output
        assert "changes detected" in log_output
        assert "No changes detected" not in log_output

        log_capture.truncate(0)
        log_capture.seek(0)

        # Call sync based on a file
        result_sync_1 = runner.invoke(
            cli, ["sync", "tests/integration-tests/jobs_rendered.yml", "-l"]
        )
        assert result_sync_1.exit_code == 0
        log_output = log_capture.getvalue()
        assert "-- SYNC --" in log_output
        assert "| ERROR" not in log_output

        log_capture.truncate(0)
        log_capture.seek(0)

        # Call plan based on a file again
        result_plan_2 = runner.invoke(
            cli, ["plan", "tests/integration-tests/jobs_rendered.yml", "-l"]
        )
        assert result_plan_2.exit_code == 0
        log_output = log_capture.getvalue()
        assert "-- PLAN --" in log_output
        assert "No changes detected" in log_output

        log_capture.truncate(0)
        log_capture.seek(0)

        # Call sync based on a file again
        result_sync_2 = runner.invoke(
            cli, ["sync", "tests/integration-tests/jobs_rendered.yml", "-l"]
        )
        assert result_sync_2.exit_code == 0
        log_output = log_capture.getvalue()
        assert "-- SYNC --" in log_output
        assert "No changes detected" in log_output

    finally:
        # always clean, even on failure
        clean_up_jobs()
