import json

from beartype.typing import Any, Dict
from pydantic import BaseModel

from src.schemas import JobDefinition


class Config(BaseModel):
    """Internal representation of a Jobs as Code configuration file."""

    jobs: Dict[str, JobDefinition]

    def __init__(self, **data: Any):
        # Check for instances where account_id is missing from a job, and add it from the config data.
        for identifier, job in data.get("jobs", dict()).items():
            if "account_id" not in job or job["account_id"] is None:
                job["account_id"] = data["account_id"]

        super().__init__(**data)


def generate_config_schema() -> str:
    json_schema = Config.model_json_schema()
    return json.dumps(json_schema, indent=2)
