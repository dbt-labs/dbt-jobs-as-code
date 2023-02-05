import re
from typing import List, Optional, Any

import pydantic

from .common_types import Execution, Settings, Schedule, Time, Triggers
from .custom_environment_variable import CustomEnvironmentVariable


# Main model for loader
class JobDefinition(pydantic.BaseModel):
    """A definition for a dbt Cloud job."""

    id: Optional[int]
    identifier: Optional[str]
    account_id: int
    project_id: int
    environment_id: int
    dbt_version: Optional[str]
    name: str
    settings: Settings
    execution: Execution = Execution()
    deferring_job_definition_id: Optional[int]
    run_generate_sources: bool
    execute_steps: List[str]
    generate_docs: bool
    schedule: Schedule
    triggers: Triggers
    state: int = 1
    custom_environment_variables: Optional[List[CustomEnvironmentVariable]] = []

    def __init__(self, **data: Any):

        # Check if `name` includes an identifier. If yes, set the identifier in the object. Remove the identifier from
        # the name.
        matches = re.search(r"\[\[([a-zA-Z0-9_]+)\]\]", data["name"])
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

    class Config:
        json_encoders = {Time: lambda t: t.serialize()}

    def to_payload(self):
        """Create a dbt Cloud API payload for a JobDefinition."""

        # Rewrite the job name to embed the job ID from job.yml
        payload = self.copy()
        payload.name = f"{self.name} [[{self.identifier}]]"
        return payload.json(exclude={"identifier", "custom_environment_variables"})

    def to_load_format(self):
        """Generate a dict following our YML format to dump as YML later."""

        data = self.dict(
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
