import pydantic

import datetime
from typing import Any, Dict, List, Optional

from . import custom_environment_variable

class JobDefinition(pydantic.BaseModel):
    identifier: str
    account_id: int
    dbt_version: str = "1.3.1"
    deferring_job_definition_id: int
    environment_id: int
    execute_steps: List[str] = ["dbt run",]
    execution: Optional[Execution]
    generate_docs: bool
    generate_sources: bool
    name: str = "New Job"
    project_id: int
    run_generate_sources: bool
    schedule: Optional[Schedule]
    settings: Optional[Settings]
    triggers: Triggers
    custom_environment_variables: List[
        custom_environment_variable.CustomEnvironmentVariable
    ] = []

class Execution(BaseModel):
    timeout_seconds: int

class Triggers(BaseModel):
    github_webhook: bool
    git_provider_webhook: Optional[bool]
    custom_branch_only: bool
    schedule: Optional[bool]

class Settings(BaseModel):
    threads: int
    target_name: str

class Schedule(BaseModel):
    cron: str
    date: datetime.date
    time: datetime.time
