from beartype.typing import Any, Literal, Optional
from pydantic import BaseModel, Field


class CustomEnvironmentVariable(BaseModel):
    name: str
    type: Literal["project", "environment", "job", "user"] = "job"
    value: Optional[str] = None
    display_value: Optional[str] = None
    job_definition_id: Optional[int] = None

    def do_validate(self):
        if not self.value:
            return "Value must be defined!"
        if not self.name.startswith("DBT_"):
            return "Key must have `DBT_` prefix."
        if not self.name.isupper():
            return "Key name must be SCREAMING_SNAKE_CASE"
        return None


class CustomEnvironmentVariablePayload(CustomEnvironmentVariable):
    """A dbt Cloud-serializable representation of a CustomEnvironmentVariables."""

    id: Optional[int] = None
    project_id: int
    account_id: int
    raw_value: Optional[str] = None
    value: Optional[str] = Field(None, exclude=True)

    def __init__(self, **data: Any):
        data["raw_value"] = data["value"] if "value" in data else data["display_value"]
        super().__init__(**data)
