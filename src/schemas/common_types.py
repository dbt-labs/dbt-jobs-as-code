from typing import Any, Dict, Optional

import pydantic
from croniter import croniter


class Execution(pydantic.BaseModel):
    timeout_seconds: int = 0


class Triggers(pydantic.BaseModel):
    github_webhook: bool
    git_provider_webhook: Optional[bool]
    custom_branch_only: bool
    schedule: Optional[bool]


class Settings(pydantic.BaseModel):
    threads: int = 4
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
    date: Optional[Date]
    time: Optional[Time]

    def __init__(self, **data: Any):
        """Defaults to the same value as the UI."""
        data["date"] = Date(type="custom_cron", cron=data["cron"])
        data["time"] = Time(type="every_hour", interval=1)
        super().__init__(**data)

    @pydantic.validator("cron")
    def valid_cron(cls, v):
        if not croniter.is_valid(v):
            raise ValueError("The cron expression is not valid")
        return v

    class Config:
        json_encoders = {Time: lambda t: t.serialize()}
