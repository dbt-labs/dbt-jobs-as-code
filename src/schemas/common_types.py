from typing import Any, Dict, Optional

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
    cron: Optional[str]
    # TODO: Redo this one!
    # days: Optional[List[int]]


class Time(pydantic.BaseModel):
    type: str
    interval: Optional[int]
    # TODO: Add this field back in
    # hours: Optional[List[int]]

    def serialize(self):
        payload: Dict[str, Any] = {"type": self.type}
        if self.type == "every_hour":
            payload["interval"] = self.interval
        elif self.type == "at_exact_hours":
            payload["hours"] = self.hours
        return payload


class Schedule(pydantic.BaseModel):
    cron: str
    date: Date
    time: Time

    class Config:
        json_encoders = {Time: lambda t: t.serialize()}
