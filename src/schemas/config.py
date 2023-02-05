from typing import Dict, Any

import pydantic

from src.schemas import JobDefinition


class Config(pydantic.BaseModel):
    """Internal representation of a Jobs as Code configuration file."""

    jobs: Dict[str, JobDefinition]

    def __init__(self, **data: Any):

        # Check for instances where account_id is missing from a job, and add it from the config data.
        for identifier, job in data.get("jobs", dict()).items():
            if "account_id" not in job or job["account_id"] is None:
                job["account_id"] = data["account_id"]

        super().__init__(**data)
