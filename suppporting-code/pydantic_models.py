from __future__ import annotations
from pydantic import BaseModel
from typing import Any, List, Optional

# The type of the data for answers from the dbt API
class DbtJobAnswer(BaseModel):
    status: Status
    data: List[DbtJob]
    extra: Extra

class Execution(BaseModel):
    timeout_seconds: int

class Extra(BaseModel):
    filters: Filters
    order_by: str
    pagination: Pagination

class Pagination(BaseModel):
    count: int
    total_count: int

class Filters(BaseModel):
    account_id: int
    limit: int
    offset: int

class Status(BaseModel):
    code: int
    is_success: bool
    user_message: str
    developer_message: str

class Triggers(BaseModel):
    github_webhook: bool
    git_provider_webhook: Optional[bool]
    custom_branch_only: bool
    schedule: Optional[bool]

class Schedule(BaseModel):
    cron: str
    date: Date
    time: Time

class Settings(BaseModel):
    threads: int
    target_name: str

class Date(BaseModel):
    type: str
    cron: Optional[str] = None
    days: Optional[List[int]] = None


class Time(BaseModel):
    type: str
    interval: Optional[int] = None
    hours: Optional[List[int]] = None

# model for interacting with our APIs
# it is part of the GET answer and is going to be added to the POST/PUT to create/update jobs
class DbtJob(BaseModel): 
    execution: Optional[Execution]
    generate_docs: Optional[bool]
    run_generate_sources: Optional[bool]
    id: Optional[int]
    account_id: int
    project_id: int
    environment_id: Optional[int]
    name: str
    dbt_version: Any
    created_at: Optional[str]
    updated_at: Optional[str]
    execute_steps: Optional[List[str]]
    state: int
    deactivated: Optional[bool]
    run_failure_count: Optional[int]
    deferring_job_definition_id: Optional[int]
    lifecycle_webhooks: Optional[bool]
    lifecycle_webhooks_url: Any
    triggers: Triggers
    settings: Optional[Settings]
    schedule: Optional[Schedule]
    is_deferrable: Optional[bool]
    generate_sources: Optional[bool]
    cron_humanized: Optional[str]
    next_run: Optional[str]
    next_run_humanized: Optional[str]