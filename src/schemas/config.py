from typing import List, Dict

import pydantic

from schemas import JobDefinition


class Config(pydantic.BaseModel):
    """Internal representation of a Jobs as Code configuration file."""

    account_id: int
    jobs: Dict[str, JobDefinition]
