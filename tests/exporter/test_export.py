import json

import pytest
from jsonschema import validate
from ruamel.yaml import YAML

from dbt_jobs_as_code.exporter.export import apply_templated_fields, export_jobs_yml
from dbt_jobs_as_code.schemas.common_types import (
    Date,
    Execution,
    Schedule,
    Settings,
    Time,
    Triggers,
)
from dbt_jobs_as_code.schemas.job import JobDefinition


@pytest.fixture
def base_job_definition():
    return JobDefinition(
        account_id=1,
        project_id=1,
        environment_id=1,
        name="Test Job",
        settings={},
        run_generate_sources=False,
        execute_steps=[],
        generate_docs=False,
        schedule={"cron": "0 14 * * 0,1,2,3,4,5,6"},
        triggers={},
    )


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


def test_export_jobs_yml_with_identifier(base_job_definition, capsys):
    # Create a job with identifier
    job_with_identifier = base_job_definition.model_copy()
    job_with_identifier.identifier = "existing_identifier"

    # Create a job without identifier
    job_without_identifier = base_job_definition.model_copy()

    jobs = [job_with_identifier, job_without_identifier]

    # Export jobs and capture output
    export_jobs_yml(jobs)
    captured = capsys.readouterr()

    # Parse the YAML output (skipping the first two lines which contain the schema)
    yaml = YAML()
    exported_jobs = yaml.load("\n".join(captured.out.split("\n")[2:]))

    # Verify the job keys
    assert "existing_identifier" in exported_jobs["jobs"]
    assert "import_2" in exported_jobs["jobs"]

    # Verify the job contents
    assert exported_jobs["jobs"]["existing_identifier"]["name"] == "Test Job"
    assert exported_jobs["jobs"]["import_2"]["name"] == "Test Job"


def test_export_jobs_yml_with_linked_id(base_job_definition, capsys):
    # Create a job with both identifier and id
    job = base_job_definition.model_copy()
    job.identifier = "test_identifier"
    job.id = 123

    # Export with include_linked_id=True
    export_jobs_yml([job], include_linked_id=True)
    captured = capsys.readouterr()

    yaml = YAML()
    exported_jobs = yaml.load("\n".join(captured.out.split("\n")[2:]))

    # Verify linked_id is included and matches the id
    assert exported_jobs["jobs"]["test_identifier"]["linked_id"] == 123

    # Export with include_linked_id=False
    export_jobs_yml([job], include_linked_id=False)
    captured = capsys.readouterr()

    yaml = YAML()
    exported_jobs = yaml.load("\n".join(captured.out.split("\n")[2:]))

    # Verify linked_id is not included
    assert "linked_id" not in exported_jobs["jobs"]["test_identifier"]


def test_apply_templated_fields_simple():
    """Test applying simple templated fields"""
    job_dict = {"name": "test job", "environment_id": 123, "project_id": 456}

    template_config = {"environment_id": "{{ environment_id }}", "project_id": "{{ project_id }}"}

    result = apply_templated_fields(job_dict, template_config)

    assert result["environment_id"] == "{{ environment_id }}"
    assert result["project_id"] == "{{ project_id }}"
    assert result["name"] == "test job"  # Unchanged field


def test_apply_templated_fields_nested():
    """Test applying templated fields to nested structures"""
    job_dict = {"name": "test job", "triggers": {"schedule": True, "github_webhook": False}}

    template_config = {"triggers.schedule": "{{ env_name == 'prod' }}"}

    result = apply_templated_fields(job_dict, template_config)

    assert result["triggers"]["schedule"] == "{{ env_name == 'prod' }}"
    assert result["triggers"]["github_webhook"] is False  # Unchanged field
    assert result["name"] == "test job"  # Unchanged field


def test_apply_templated_fields_missing_path():
    """Test applying templated fields when the path doesn't exist"""
    job_dict = {"name": "test job"}

    template_config = {"triggers.schedule": "{{ env_name == 'prod' }}"}

    result = apply_templated_fields(job_dict, template_config)

    assert result["triggers"]["schedule"] == "{{ env_name == 'prod' }}"
    assert result["name"] == "test job"  # Unchanged field


def test_apply_templated_fields_empty_config():
    """Test applying empty template config"""
    job_dict = {"name": "test job", "environment_id": 123}

    template_config = {}

    result = apply_templated_fields(job_dict, template_config)

    assert result == job_dict  # Should return unchanged dictionary


def test_export_jobs_yml_with_templates(base_job_definition, capsys, tmp_path):
    """Test that export_jobs_yml correctly applies templates from a file"""
    # Create a temporary template file
    template_file = tmp_path / "templates.yml"
    template_config = {
        "triggers.schedule": "{{ env_name == 'prod' }}",
        "environment_id": "{{ environment_id }}",
    }

    yaml = YAML()
    with open(template_file, "w") as f:
        yaml.dump(template_config, f)

    # Create a job
    job = base_job_definition.model_copy()
    job.identifier = "test_job"

    # Export with templates
    export_jobs_yml([job], template_file=str(template_file))
    captured = capsys.readouterr()

    # We don't parse the YAML as it is not valid YAML (it is Jinja YAML and will consider { as the start of a dictionary)
    assert "schedule: {{ env_name == 'prod' }}" in captured.out
    assert "environment_id: {{ environment_id }}" in captured.out


def test_export_jobs_yml_with_curly_braces(base_job_definition, capsys, tmp_path):
    """Test that export_jobs_yml correctly handles curly braces in templates"""
    # Create a template file with curly braces
    template_file = tmp_path / "templates.yml"
    template_config = {
        "triggers.schedule": "{{ env_name == 'prod' }}",
        "environment_id": "{{ environment_id }}",
        "project_id": "{{ project_id }}",
        "deferring_environment_id": "{{ deferring_environment_id }}",
    }

    yaml = YAML()
    with open(template_file, "w") as f:
        yaml.dump(template_config, f)

    # Create a job
    job = base_job_definition.model_copy()
    job.identifier = "test_job"

    # Export with templates
    export_jobs_yml([job], template_file=str(template_file))
    captured = capsys.readouterr()

    # Verify that curly braces are preserved in the output
    assert "{{ env_name == 'prod' }}" in captured.out
    assert "{{ environment_id }}" in captured.out
    assert "{{ project_id }}" in captured.out
    assert "{{ deferring_environment_id }}" in captured.out

    # Verify that we haven't added quotes around
    assert (
        """'{{ env_name == 'prod' }}'""" not in captured.out
        and """\"{{ env_name == 'prod' }}\"""" not in captured.out
    )
    assert (
        """'{{ environment_id }}'""" not in captured.out
        and """\"{{ environment_id }}\"""" not in captured.out
    )
    assert (
        """'{{ project_id }}'""" not in captured.out
        and """\"{{project_id }}\"""" not in captured.out
    )
    assert (
        """'{{ deferring_environment_id }}'""" not in captured.out
        and """\"{{ deferring_environment_id }}\"""" not in captured.out
    )
