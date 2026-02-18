import os

import pytest
from pydantic_core import ValidationError

from dbt_jobs_as_code.client import DBTCloud

DBT_ACCOUNT_ID = int(os.getenv("DBT_ACCOUNT_ID", 0))
DBT_API_KEY = os.getenv("DBT_API_KEY")
DBT_BASE_URL = os.getenv("DBT_BASE_URL", "")
DBT_JOB_ID = int(os.getenv("DBT_JOB_ID", 0))


def test_new_job_fields():
    client = DBTCloud(
        account_id=DBT_ACCOUNT_ID,
        api_key=DBT_API_KEY,
        base_url=DBT_BASE_URL,
    )

    try:
        parsed_job = client.get_job_missing_fields(
            job_id=DBT_JOB_ID,
        )
    except ValidationError as e:
        pytest.fail(
            f"""ValidationError was raised: {e.errors()}\n\n
            This means that a field needs to be explicitly excluded in JobMissingFields or added to the JobDefinition model."""
        )
    except Exception as e:
        pytest.fail(f"A generic error was raised: {e}")

    if not parsed_job:
        pytest.fail("No job was parsed, check if it still exists")
