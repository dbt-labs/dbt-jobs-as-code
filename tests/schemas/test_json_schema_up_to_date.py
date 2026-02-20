import json
from pathlib import Path

from dbt_jobs_as_code.schemas.config import generate_config_schema

SCHEMA_PATH = Path("src/dbt_jobs_as_code/schemas/load_job_schema.json")


def test_json_schema_is_up_to_date():
    """Ensure the committed JSON schema matches what the models generate.

    If this fails, run: `dbt-jobs-as-code update-json-schema`
    """
    committed = json.loads(SCHEMA_PATH.read_text())
    generated = json.loads(generate_config_schema())
    assert committed == generated, (
        "The committed JSON schema is out of date. "
        "Run `dbt-jobs-as-code update-json-schema` to regenerate it."
    )
