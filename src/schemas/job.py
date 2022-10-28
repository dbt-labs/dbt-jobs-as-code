from typing import List, Optional

import pydantic

from .common_types import Execution, Settings, Schedule, Triggers
from .custom_environment_variable import CustomEnvironmentVariable


# Main model for loader
class JobDefinition(pydantic.BaseModel):
    """A definition for a dbt Cloud job."""
    id: Optional[int]
    identifier: Optional[str]
    account_id: int
    deferring_job_definition_id: Optional[int]
    environment_id: int
    execution: Optional[Execution]
    generate_docs: bool
    generate_sources: bool
    id: Optional[int]
    project_id: int
    run_generate_sources: bool
    schedule: Optional[Schedule]
    settings: Optional[Settings]
    triggers: Triggers
    state: int = 1

    # TODO: There's currently a defect where we can end up with multiple formatted names. Fix it!
    name: str = "New Job"
    dbt_version: Optional[str] = "1.3.1"
    execute_steps: List[str] = ["dbt run"]
    custom_environment_variables: Optional[
        List[CustomEnvironmentVariable]
    ] = []

    def to_payload(self):
        """Create a dbt Cloud API payload for a JobDefinition."""

        # Rewrite the job name to embed the job ID from job.yml
        payload = self.copy()
        payload.name = f"{self.name} [[{self.identifier}]]"
        return payload.json(exclude={'identifier', 'custom_environment_variables'})
