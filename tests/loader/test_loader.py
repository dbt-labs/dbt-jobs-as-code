import pytest
from dbt_jobs_as_code.loader.load import LoadingJobsYAMLError, load_job_configuration

# all the different ways of defining a set of jobs still create the same Pydantic jobs

expected_config_dict = {
    "jobs": {
        "job1": {
            "account_id": 43791,
            "compare_changes_flags": "--select state:modified",
            "custom_environment_variables": [],
            "dbt_version": None,
            "deferring_environment_id": None,
            "deferring_job_definition_id": None,
            "description": "",
            "environment_id": 134459,
            "execute_steps": [
                "dbt run --select model1+",
                "dbt run --select model2+",
                "dbt compile",
            ],
            "execution": {"timeout_seconds": 0},
            "generate_docs": False,
            "id": None,
            "identifier": "job1",
            "job_completion_trigger_condition": None,
            "job_type": "scheduled",
            "linked_id": None,
            "name": "My Job 1 with a new name",
            "project_id": 176941,
            "run_generate_sources": True,
            "schedule": {
                "cron": "0 */2 * * *",
                "date": {"cron": "0 */2 * * *", "type": "custom_cron"},
                "time": {"interval": 1, "type": "every_hour"},
            },
            "settings": {"target_name": "production", "threads": 4},
            "state": 1,
            "triggers": {
                "git_provider_webhook": False,
                "github_webhook": False,
                "on_merge": False,
                "schedule": True,
            },
            "triggers_on_draft_pr": False,
            "run_compare_changes": False,
        },
        "job2": {
            "account_id": 43791,
            "compare_changes_flags": "--select state:modified",
            "custom_environment_variables": [
                {
                    "display_value": None,
                    "job_definition_id": None,
                    "name": "DBT_ENV1",
                    "type": "job",
                    "value": "My val",
                },
                {
                    "display_value": None,
                    "job_definition_id": None,
                    "name": "DBT_ENV2",
                    "type": "job",
                    "value": "My val2",
                },
            ],
            "dbt_version": None,
            "deferring_environment_id": None,
            "deferring_job_definition_id": None,
            "description": "",
            "environment_id": 134459,
            "execute_steps": ["dbt run-operation clone_all_production_schemas", "dbt compile"],
            "execution": {"timeout_seconds": 0},
            "generate_docs": True,
            "id": None,
            "identifier": "job2",
            "job_completion_trigger_condition": {
                "condition": {"job_id": 123, "project_id": 234, "statuses": [10, 20]}
            },
            "job_type": "other",
            "linked_id": None,
            "name": "CI/CD run",
            "project_id": 176941,
            "run_generate_sources": True,
            "schedule": {
                "cron": "0 * * * *",
                "date": {"cron": "0 * * * *", "type": "custom_cron"},
                "time": {"interval": 1, "type": "every_hour"},
            },
            "settings": {"target_name": "TEST", "threads": 4},
            "state": 1,
            "triggers": {
                "git_provider_webhook": False,
                "github_webhook": True,
                "on_merge": True,
                "schedule": False,
            },
            "triggers_on_draft_pr": True,
            "run_compare_changes": True,
        },
    }
}


def test_load_yml_no_anchor():
    """Test that loading configuration without YML anchors works as expected."""

    with open("tests/loader/jobs.yml") as file:
        loaded_config = load_job_configuration(file, None)

    assert loaded_config.model_dump() == expected_config_dict


def test_load_yml_anchors():
    """Test that loading configuration with YML anchors works as expected."""

    with open("tests/loader/jobs_with_anchors.yml") as file:
        loaded_config = load_job_configuration(file, None)

    assert loaded_config.model_dump() == expected_config_dict


def test_load_yml_templated():
    """Test that load_job_configuration works with templated YAML and variables."""

    with open("tests/loader/jobs_templated.yml") as file:
        with open("tests/loader/jobs_templated_vars.yml") as templated_file:
            loaded_config = load_job_configuration(file, templated_file)

    assert loaded_config.model_dump() == expected_config_dict


def test_error_load_yml_templated_missing_vars_parameter():
    """Test that load_job_configuration works with templated YAML and variables."""

    with pytest.raises(LoadingJobsYAMLError) as exc_info:
        with open("tests/loader/jobs_templated.yml") as file:
            with open("tests/loader/jobs_templated_vars_missing.yml") as templated_file:
                load_job_configuration(file, templated_file)

    # we check that the error messages contains the missing variables
    assert "'environment_id'" in str(exc_info.value)


def test_error_load_yml_templated_missing_specific_var():
    """Test that load_job_configuration works with templated YAML and variables."""

    with pytest.raises(LoadingJobsYAMLError) as exc_info:
        with open("tests/loader/jobs_templated.yml") as file:
            load_job_configuration(file, None)

    # we check that the error messages contains the missing variables
    assert "'environment_id'" in str(exc_info.value)
    assert "'project_id'" in str(exc_info.value)
