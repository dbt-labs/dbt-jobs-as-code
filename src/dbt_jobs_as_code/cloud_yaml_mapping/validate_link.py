from dataclasses import dataclass
from typing import Optional

from dbt_jobs_as_code.client import DBTCloud, DBTCloudException
from dbt_jobs_as_code.schemas.job import JobDefinition


@dataclass
class LinkableCheck:
    can_be_linked: bool
    message: str
    linked_job: Optional[JobDefinition] = None


def can_be_linked(
    job_identifier: str, job_definition: JobDefinition, dbt_cloud: DBTCloud
) -> LinkableCheck:
    if job_definition.linked_id is None:
        return LinkableCheck(
            False, f"Job '{job_identifier}' doesn't have an ID in YAML. It cannot be linked"
        )

    try:
        cloud_job = dbt_cloud.get_job(job_id=job_definition.linked_id)
    except DBTCloudException:
        return LinkableCheck(
            False,
            f"Job {job_definition.linked_id} doesn't exist in dbt Cloud. It cannot be linked",
        )

    if cloud_job.identifier is not None:
        return LinkableCheck(
            False,
            f"Job {job_definition.linked_id} is already linked with the identifier {cloud_job.identifier}. You should unlink it before if you want to link it to a new identifier.",
        )

    return LinkableCheck(True, "", cloud_job)
