from typing import List, Optional

import pydantic

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