import pydantic

from typing import Literal, Optional


class CustomEnvironmentVariable(pydantic.BaseModel):
    account_id: int
    project_id: int
    name: str = "DBT_VARIABLE"
    type: Literal["project", "environment", "job", "user"] = "job"

    value: Optional[str]

    def do_validate(self):
        if not self.value:
            return "Value must be defined!"
        if not self.name.startswith("DBT_"):
            return "Key must have `DBT_` prefix."
        if not self.name.isupper():
            return "Key name must be SCREAMING_SNAKE_CASE"
        return None
