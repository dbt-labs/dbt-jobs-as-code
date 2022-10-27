import pydantic

from typing import Any, Dict, List, Optional

from . import custom_environment_variable


# Helper Models

class Execution(pydantic.BaseModel):
    timeout_seconds: int

class Triggers(pydantic.BaseModel):
    github_webhook: bool
    git_provider_webhook: Optional[bool]
    custom_branch_only: bool
    schedule: Optional[bool]

class Settings(pydantic.BaseModel):
    threads: int
    target_name: str

class Date(pydantic.BaseModel):
    type: str
    cron: Optional[str] = None
    days: Optional[List[int]] = None

class Time(pydantic.BaseModel):
    type: str
    interval: Optional[int] = None
    hours: Optional[List[int]] = None

class Schedule(pydantic.BaseModel):
    cron: str
    date: Date
    time: Time

# Main model for loader
class JobDefinition(pydantic.BaseModel):
    identifier: str
    account_id: int
    dbt_version: Optional[str] = "1.3.1"
    deferring_job_definition_id: Optional[int]
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
    custom_environment_variables: Optional[List[
        custom_environment_variable.CustomEnvironmentVariable
    ]] = []
