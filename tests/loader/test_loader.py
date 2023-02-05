import json
from loader.load import load_job_configuration
from ruamel.yaml import YAML


from exporter.export import export_jobs_yml
from jsonschema import validate
from schemas.config import Config
from schemas.custom_environment_variable import CustomEnvironmentVariable
from src.schemas.common_types import Date, Execution, Schedule, Settings, Time, Triggers

from src.schemas.job import JobDefinition


def test_import_yml_no_anchor():
    """Test that load_job_configuration works as expected."""

    expected_config_dict = {
        "jobs": {
            "job1": {
                "id": None,
                "identifier": "job1",
                "account_id": 43791,
                "project_id": 176941,
                "environment_id": 134459,
                "dbt_version": None,
                "name": "My Job 1",
                "settings": {"threads": 4, "target_name": "production"},
                "execution": {"timeout_seconds": 0},
                "deferring_job_definition_id": None,
                "run_generate_sources": True,
                "execute_steps": [
                    "dbt run --select model1+",
                    "dbt run --select model2+",
                ],
                "generate_docs": False,
                "schedule": {
                    "cron": "0 */2 * * *",
                    "date": {"type": "custom_cron", "cron": "0 */2 * * *"},
                    "time": {"type": "every_hour", "interval": 1},
                },
                "triggers": {
                    "github_webhook": False,
                    "git_provider_webhook": False,
                    "custom_branch_only": True,
                    "schedule": True,
                },
                "state": 1,
                "custom_environment_variables": [],
            },
            "job2": {
                "id": None,
                "identifier": "job2",
                "account_id": 43791,
                "project_id": 176941,
                "environment_id": 134459,
                "dbt_version": None,
                "name": "CI/CD run",
                "settings": {"threads": 4, "target_name": "TEST"},
                "execution": {"timeout_seconds": 0},
                "deferring_job_definition_id": 43791,
                "run_generate_sources": False,
                "execute_steps": [
                    "dbt run-operation clone_all_production_schemas",
                    "dbt compile",
                ],
                "generate_docs": False,
                "schedule": {
                    "cron": "0 * * * *",
                    "date": {"type": "custom_cron", "cron": "0 * * * *"},
                    "time": {"type": "every_hour", "interval": 1},
                },
                "triggers": {
                    "github_webhook": True,
                    "git_provider_webhook": False,
                    "custom_branch_only": True,
                    "schedule": False,
                },
                "state": 1,
                "custom_environment_variables": [
                    {
                        "name": "DBT_ENV1",
                        "type": "job",
                        "value": "My val",
                        "display_value": None,
                        "job_definition_id": None,
                    },
                    {
                        "name": "DBT_ENV2",
                        "type": "job",
                        "value": "My val2",
                        "display_value": None,
                        "job_definition_id": None,
                    },
                ],
            },
        }
    }

    with open("tests/loader/jobs.yml") as file:
        loaded_config = load_job_configuration(file)

    assert loaded_config.dict() == expected_config_dict


def test_import_yml_anchors():
    """Test that load_job_configuration works as expected."""

    expected_config_dict = {
        "jobs": {
            "job1": {
                "id": None,
                "identifier": "job1",
                "account_id": 43791,
                "project_id": 176941,
                "environment_id": 134459,
                "dbt_version": None,
                "name": "My Job 1 with a new name",
                "settings": {"threads": 4, "target_name": "production"},
                "execution": {"timeout_seconds": 0},
                "deferring_job_definition_id": None,
                "run_generate_sources": True,
                "execute_steps": [
                    "dbt run --select model1+",
                    "dbt run --select model2+",
                    "dbt compile",
                ],
                "generate_docs": False,
                "schedule": {
                    "cron": "0 */2 * * *",
                    "date": {"type": "custom_cron", "cron": "0 */2 * * *"},
                    "time": {"type": "every_hour", "interval": 1},
                },
                "triggers": {
                    "github_webhook": False,
                    "git_provider_webhook": False,
                    "custom_branch_only": True,
                    "schedule": True,
                },
                "state": 1,
                "custom_environment_variables": [],
            },
            "job2": {
                "id": None,
                "identifier": "job2",
                "account_id": 43791,
                "project_id": 176941,
                "environment_id": 134459,
                "dbt_version": None,
                "name": "CI/CD run",
                "settings": {"threads": 4, "target_name": "TEST"},
                "execution": {"timeout_seconds": 0},
                "deferring_job_definition_id": None,
                "run_generate_sources": True,
                "execute_steps": [
                    "dbt run-operation clone_all_production_schemas",
                    "dbt compile",
                ],
                "generate_docs": True,
                "schedule": {
                    "cron": "0 * * * *",
                    "date": {"type": "custom_cron", "cron": "0 * * * *"},
                    "time": {"type": "every_hour", "interval": 1},
                },
                "triggers": {
                    "github_webhook": True,
                    "git_provider_webhook": False,
                    "custom_branch_only": True,
                    "schedule": False,
                },
                "state": 1,
                "custom_environment_variables": [
                    {
                        "name": "DBT_ENV1",
                        "type": "job",
                        "value": "My val",
                        "display_value": None,
                        "job_definition_id": None,
                    },
                    {
                        "name": "DBT_ENV2",
                        "type": "job",
                        "value": "My val2",
                        "display_value": None,
                        "job_definition_id": None,
                    },
                ],
            },
        }
    }

    with open("tests/loader/jobs_with_anchors.yml") as file:
        loaded_config = load_job_configuration(file)

    assert loaded_config.dict() == expected_config_dict
