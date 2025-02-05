from beartype.typing import List, Optional
from loguru import logger

from dbt_jobs_as_code.client import DBTCloud
from dbt_jobs_as_code.loader.load import load_job_configuration
from dbt_jobs_as_code.schemas.job import JobDefinition


def get_account_id(config_files: Optional[List[str]], account_id: Optional[int]) -> int:
    """Get account ID from either config file or direct input"""
    if account_id:
        return account_id
    elif config_files:
        defined_jobs = load_job_configuration(config_files, None).jobs.values()
        return list(defined_jobs)[0].account_id
    else:
        raise ValueError("Either config or account_id must be provided")


def check_job_fields(dbt_cloud: DBTCloud, job_ids: List[int]) -> None:
    """Check if there are new fields in job model"""
    if not job_ids:
        logger.error("We need to provide some job_id to test the import")
        return

    logger.info("Checking if there are new fields for jobs")
    dbt_cloud.get_job_missing_fields(job_id=job_ids[0])


def fetch_jobs(
    dbt_cloud: DBTCloud, job_ids: List[int], project_ids: List[int], environment_ids: List[int]
) -> List[JobDefinition]:
    """Fetch jobs from dbt Cloud based on provided filters"""
    logger.info("Getting the jobs definition from dbt Cloud")

    if job_ids and not (project_ids or environment_ids):
        # Get jobs one by one if only job_ids provided
        cloud_jobs_can_have_none = [dbt_cloud.get_job(job_id=id) for id in job_ids]
        return [job for job in cloud_jobs_can_have_none if job is not None]

    # Get all jobs and filter
    cloud_jobs = dbt_cloud.get_jobs(project_ids=project_ids, environment_ids=environment_ids)
    if job_ids:
        cloud_jobs = [job for job in cloud_jobs if job.id in job_ids]
    return cloud_jobs
