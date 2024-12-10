from beartype.typing import Any, Literal, Optional
from pydantic import BaseModel, Field, model_validator
from typing_extensions import Self


class CustomEnvironmentVariable(BaseModel):
    name: str
    type: Literal["project", "environment", "job", "user"] = "job"
    value: Optional[str] = Field(default=None)
    display_value: Optional[str] = None
    job_definition_id: Optional[int] = None

    @model_validator(mode="after")
    def check_env_var(self) -> Self:
        if not self.name.startswith("DBT_"):
            raise ValueError("Key must have `DBT_` prefix.")
        if not self.name.isupper():
            raise ValueError("Key name must be SCREAMING_SNAKE_CASE")
        return self


class CustomEnvironmentVariablePayload(CustomEnvironmentVariable):
    """A dbt Cloud-serializable representation of a CustomEnvironmentVariables."""

    id: Optional[int] = None
    project_id: int
    account_id: int
    raw_value: Optional[str] = None
    value: Optional[str] = Field(default=None, exclude=True)

    def __init__(self, **data: Any):
        data["raw_value"] = data["value"] if "value" in data else data["display_value"]
        super().__init__(**data)
