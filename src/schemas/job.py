import pydantic

from typing import Any, Dict, List, Optional

from .common_types import Execution, Settings, Schedule, Triggers
from .custom_environment_variable import CustomEnvironmentVariable


# Main model for loader
class JobDefinition(pydantic.BaseModel):
    id: Optional[int]
    identifier: str
    account_id: int
    dbt_version: Optional[str] = "1.3.1"
    deferring_job_definition_id: Optional[int]
    environment_id: int
    execute_steps: List[str] = ["dbt run",]
    execution: Optional[Execution]
    generate_docs: bool
    generate_sources: bool
    id: Optional[int]
    name: str = "New Job"
    project_id: int
    run_generate_sources: bool
    schedule: Optional[Schedule]
    settings: Optional[Settings]
    triggers: Triggers
    state: int = 1
    custom_environment_variables: Optional[
        List[CustomEnvironmentVariable]
    ] = []


    def to_payload(self):
        """Create a dbt Cloud API payload for a JobDefintion."""
        # TODO: Tweak name to use the format `{name} [{identifier}]`
        return self.json(exclude={'identifier', 'custom_environment_variables'})
