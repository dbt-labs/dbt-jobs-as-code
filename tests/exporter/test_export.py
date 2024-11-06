import json

from dbt_jobs_as_code.exporter.export import export_jobs_yml
from dbt_jobs_as_code.schemas.common_types import (
    Date,
    Execution,
    Schedule,
    Settings,
    Time,
    Triggers,
)
from dbt_jobs_as_code.schemas.job import JobDefinition
from jsonschema import validate
from ruamel.yaml import YAML


def test_export_jobs_yml(capsys):
    """Test that the export_jobs_yml function works as expected."""

    expected_output = """
# yaml-language-server: $schema=https://raw.githubusercontent.com/dbt-labs/dbt-jobs-as-code/main/src/dbt_jobs_as_code/schemas/load_job_schema.json

jobs:
  import_1:
    account_id: 50
    project_id: 100
    environment_id: 248
    dbt_version:
    name: Event Model
    settings:
      threads: 4
      target_name: production
    execution:
      timeout_seconds: 0
    deferring_job_definition_id:
    deferring_environment_id:
    run_generate_sources: true
    execute_steps:
      - dbt source freshness
      - dbt build
    generate_docs: false
    schedule:
      cron: 0 14 * * 0,1,2,3,4,5,6
    triggers:
      github_webhook: false
      git_provider_webhook: false
      schedule: false
      on_merge: false
    description: ''
    run_compare_changes: false
    compare_changes_flags: --select state:modified
    job_type: scheduled
    triggers_on_draft_pr: false
    job_completion_trigger_condition:
    custom_environment_variables: []
""".strip()

    job_def = JobDefinition(
        id=31,
        identifier=None,
        account_id=50,
        project_id=100,
        environment_id=248,
        dbt_version=None,
        name="Event Model",
        settings=Settings(threads=4, target_name="production"),
        execution=Execution(timeout_seconds=0),
        deferring_job_definition_id=None,
        deferring_environment_id=None,
        run_generate_sources=True,
        execute_steps=[
            "dbt source freshness",
            "dbt build",
        ],
        generate_docs=False,
        schedule=Schedule(
            cron="0 14 * * 0,1,2,3,4,5,6",
            date=Date(type="custom_cron", cron="0 14 * * 0,1,2,3,4,5,6"),
            time=Time(type="every_hour", interval=1),
        ),
        triggers=Triggers(
            github_webhook=False,
            git_provider_webhook=False,
            schedule=False,
        ),
        state=1,
        custom_environment_variables=[],
        description="",
    )

    export_jobs_yml([job_def])
    captured = capsys.readouterr()
    assert captured.out.strip() == expected_output

    # checking that the output YAML is a valid job
    with open("src/dbt_jobs_as_code/schemas/load_job_schema.json") as f:
        schema = f.read()

    yaml = YAML(typ="safe")
    yaml_data = yaml.load(captured.out.strip())
    validate(instance=yaml_data, schema=json.loads(schema))
