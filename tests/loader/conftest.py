import pytest


@pytest.fixture
def single_config_content():
    return """
jobs:
  job1:
    project_id: {{ project_id }}
    environment_id: {{ env_id }}
"""


@pytest.fixture
def single_vars_content():
    return """
project_id: 123
env_id: 456
"""


@pytest.fixture
def config_files(tmp_path, single_config_content):
    config_file = tmp_path / "config.yml"
    config_file.write_text(single_config_content)
    return [str(config_file)]


@pytest.fixture
def vars_files(tmp_path, single_vars_content):
    vars_file = tmp_path / "vars.yml"
    vars_file.write_text(single_vars_content)
    return [str(vars_file)]


@pytest.fixture
def multiple_config_files(tmp_path):
    config1 = tmp_path / "config1.yml"
    config2 = tmp_path / "config2.yml"

    config1.write_text("""
jobs:
  job1:
    project_id: {{ project_id }}
""")

    config2.write_text("""
jobs:
  job2:
    environment_id: {{ env_id }}
""")

    return [str(config1), str(config2)]


@pytest.fixture
def multiple_vars_files(tmp_path):
    vars1 = tmp_path / "vars1.yml"
    vars2 = tmp_path / "vars2.yml"

    vars1.write_text("project_id: 123")
    vars2.write_text("env_id: 456")

    return [str(vars1), str(vars2)]


@pytest.fixture
def expected_config_dict():
    return {
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
