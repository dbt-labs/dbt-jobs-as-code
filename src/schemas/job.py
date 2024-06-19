import re

from beartype.typing import Any, List, Optional
from pydantic import BaseModel, ConfigDict, Field

from .common_types import (
    Execution,
    JobCompletionTriggerCondition,
    Schedule,
    Settings,
    Time,
    Triggers,
)
from .custom_environment_variable import CustomEnvironmentVariable


# Main model for loader
class JobDefinition(BaseModel):
    """A definition for a dbt Cloud job."""

    id: Optional[int] = None
    identifier: Optional[str] = None
    account_id: int
    project_id: int
    environment_id: int
    dbt_version: Optional[str] = None
    name: str
    settings: Settings
    execution: Execution = Execution()
    deferring_job_definition_id: Optional[int] = None
    deferring_environment_id: Optional[int] = None
    run_generate_sources: bool
    execute_steps: List[str]
    generate_docs: bool
    schedule: Schedule
    triggers: Triggers
    description: str = ""
    state: int = 1
    custom_environment_variables: List[CustomEnvironmentVariable] = []

    def __init__(self, **data: Any):
        # Check if `name` includes an identifier. If yes, set the identifier in the object. Remove the identifier from
        # the name.
        matches = re.search(r"\[\[([a-zA-Z0-9_-]+)\]\]", data["name"])
        if matches is not None:
            data["identifier"] = matches.groups()[0]
            data["name"] = data["name"].replace(f" [[{data['identifier']}]]", "")

        # Rewrite custom environment variables to include account and project id
        environment_variables = data.get("custom_environment_variables", None)
        if environment_variables:
            data["custom_environment_variables"] = [
                {
                    "name": list(variable.keys())[0],
                    "value": list(variable.values())[0],
                    "project_id": data["project_id"],
                    "account_id": data["account_id"],
                }
                for variable in environment_variables
            ]
        else:
            data["custom_environment_variables"] = []

        super().__init__(**data)

    def to_payload(self):
        """Create a dbt Cloud API payload for a JobDefinition."""

        # Rewrite the job name to embed the job ID from job.yml
        payload = self.model_copy()
        # if there is an identifier, add it to the name
        # otherwise, it means that we are "unlinking" the job from the job.yml
        if self.identifier:
            payload.name = f"{self.name} [[{self.identifier}]]"
        return payload.model_dump_json(exclude={"identifier", "custom_environment_variables"})

    def to_load_format(self):
        """Generate a dict following our YML format to dump as YML later."""

        data = self.model_dump(
            exclude={
                "identifier": True,
                "schedule": {
                    "date": True,
                    "time": True,
                },
                "custom_environment_variables": True,
                "id": True,
                "state": True,
            }
        )
        data["custom_environment_variables"] = []
        for env_var in self.custom_environment_variables:
            data["custom_environment_variables"].append({env_var.name: env_var.value})
        return data


class JobMissingFields(JobDefinition):
    """This class can be used to identify when there are new fields added to jobs
    We don't add the not needed fields to the JobDefinition model to prevent the tool from breaking with any API change
    """

    model_config = ConfigDict(extra="forbid")

    # when adding fields we also need to update the test for pytest

    # TODO: Add to JobDefinition model when the feature is out
    run_compare_changes: bool = False

    # Unneeded read-only fields
    raw_dbt_version: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""
    deactivated: bool
    run_failure_count: int = False
    lifecycle_webhooks: bool = False
    lifecycle_webhooks_url: Optional[str] = None
    is_deferrable: bool = False
    generate_sources: bool = False
    cron_humanized: str = ""
    next_run: Optional[str] = ""
    next_run_humanized: Optional[str] = ""
