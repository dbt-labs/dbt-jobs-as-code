from beartype.typing import Any, Dict, List, Literal, Optional
from croniter import croniter
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator


def set_one_of_string_integer(schema: Dict[str, Any]):
    schema.pop("type", None)
    schema["oneOf"] = [{"type": "string"}, {"type": "integer"}]


def set_any_of_string_integer_null(schema: Dict[str, Any]):
    schema.pop("type", None)
    schema["anyOf"] = [{"type": "string"}, {"type": "integer"}, {"type": "null"}]


field_mandatory_int_allowed_as_string_in_schema = Field(
    json_schema_extra=set_one_of_string_integer
)
field_optional_int_allowed_as_string_in_schema = Field(
    default=None, json_schema_extra=set_any_of_string_integer_null
)


class Execution(BaseModel):
    timeout_seconds: int = 0


def set_any_of_string_boolean(schema: Dict[str, Any]):
    schema.pop("type", None)
    schema["anyOf"] = [{"type": "string"}, {"type": "boolean"}]


field_optional_bool_allowed_as_string_in_schema = Field(
    default=False, json_schema_extra=set_any_of_string_boolean
)


class Triggers(BaseModel):
    github_webhook: bool = field_optional_bool_allowed_as_string_in_schema
    git_provider_webhook: Optional[bool] = field_optional_bool_allowed_as_string_in_schema
    schedule: Optional[bool] = field_optional_bool_allowed_as_string_in_schema
    on_merge: Optional[bool] = field_optional_bool_allowed_as_string_in_schema


class Settings(BaseModel):
    threads: int = 4
    target_name: str = "default"


class Date(BaseModel):
    type: str
    cron: Optional[str] = None
    # TODO: Redo this one!
    # days: Optional[List[int]]


class Time(BaseModel):
    type: str
    interval: Optional[int] = None
    # TODO: Add this field back in
    # hours: Optional[List[int]] = []

    def serialize(self):
        payload: Dict[str, Any] = {"type": self.type}
        if self.type == "every_hour":
            payload["interval"] = self.interval
        elif self.type == "at_exact_hours":
            payload["hours"] = self.hours  # type: ignore
        return payload


class Schedule(BaseModel):
    cron: str
    date: Optional[Date] = None
    time: Optional[Time] = None

    def __init__(self, **data: Any):
        """Defaults to the same value as the UI."""
        data["date"] = Date(type="custom_cron", cron=data["cron"])
        data["time"] = Time(type="every_hour", interval=1)
        super().__init__(**data)

    @field_validator("cron")
    def valid_cron(cls, v):
        if not croniter.is_valid(v):
            raise ValueError("The cron expression is not valid")
        return v

    @field_serializer("time", when_used="json")
    def serialize_field(time: Optional[Time]):  # type: ignore
        if time is None:
            return None
        return time.serialize()


class Condition(BaseModel):
    job_id: int = field_mandatory_int_allowed_as_string_in_schema
    project_id: int = field_mandatory_int_allowed_as_string_in_schema
    statuses: List[Literal[10, 20, 30]] = Field(
        default=[10, 20, 30],
        description="The statuses that will trigger the job. 10=success 20=error 30=cancelled",
    )


class JobCompletionTriggerCondition(BaseModel):
    condition: Condition
