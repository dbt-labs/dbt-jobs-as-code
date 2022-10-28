from typing import List, Dict, Any

import pydantic
from loguru import logger
from pydantic import validator

from schemas import JobDefinition


class Config(pydantic.BaseModel):
    """Internal representation of a Jobs as Code configuration file."""

    account_id: int
    jobs: Dict[str, JobDefinition]

    @validator('jobs')
    def jobs_must_have_none_or_matching_account_id(cls, v, values, **kwargs):
        """Confirm that each job has either a None account_id, or that the `account_id` matches self.account_id"""
        for identifier, job in v.items():
            if job.account_id != values['account_id']:
                raise ValueError(
                    f'The job `{identifier}` contains an `account_id` that does not match the global config account_id.'
                )
        return v

    def __init__(self, **data: Any):

        # Check for instances where account_id is missing from a job, and add it from the config data.
        for identifier, job in data.get('jobs', dict()).items():
            if 'account_id' not in job or job['account_id'] is None:
                job['account_id'] = data['account_id']

        super().__init__(**data)
