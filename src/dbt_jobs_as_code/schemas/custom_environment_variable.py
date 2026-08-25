from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator
from typing_extensions import Self


class CustomEnvironmentVariable(BaseModel):
    name: str
    type: Literal["project", "environment", "job", "user"] = "job"
    value: str | None = Field(default=None)
    display_value: str | None = None
    job_definition_id: int | None = None

    @model_validator(mode="after")
    def check_env_var(self) -> Self:
        if not self.name.startswith("DBT_"):
            raise ValueError("Key must have `DBT_` prefix.")
        if not self.name.isupper():
            raise ValueError("Key name must be SCREAMING_SNAKE_CASE")
        return self


class CustomEnvironmentVariablePayload(CustomEnvironmentVariable):
    """A dbt Cloud-serializable representation of a CustomEnvironmentVariables."""

    id: int | None = None
    project_id: int
    account_id: int
    raw_value: str | None = None
    value: str | None = Field(default=None, exclude=True)

    def __init__(self, **data: Any):
        data["raw_value"] = data["value"] if "value" in data else data["display_value"]
        super().__init__(**data)
