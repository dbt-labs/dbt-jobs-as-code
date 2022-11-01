import os

from loguru import logger
import click

from client import DBTCloud
from loader.load import load_job_configuration
from schemas import check_job_mapping_same


@click.command()
@click.argument("config", type=click.File("r"))
def cli(config):
    """Synchronize a dbt Cloud job config file against dbt Cloud.

    CONFIG is the path to your jobs.yml config file.
    """
    configuration = load_job_configuration(config)
    defined_jobs = configuration.jobs

    dbt_cloud = DBTCloud(
        account_id=configuration.account_id, api_key=os.environ.get("API_KEY")
    )
    cloud_jobs = dbt_cloud.get_jobs()
    tracked_jobs = {
        job.identifier: job for job in cloud_jobs if job.identifier is not None
    }

    # Use sets to find jobs for different operations
    shared_jobs = set(defined_jobs.keys()).intersection(set(tracked_jobs.keys()))
    created_jobs = set(defined_jobs.keys()) - set(tracked_jobs.keys())
    deleted_jobs = set(tracked_jobs.keys()) - set(defined_jobs.keys())

    # Update changed jobs
    logger.info("Detected {count} existing jobs.", count=len(shared_jobs))
    for identifier in shared_jobs:
        logger.info("Checking for differences in {identifier}", identifier=identifier)
        if not check_job_mapping_same(
            source_job=defined_jobs[identifier], dest_job=tracked_jobs[identifier]
        ):
            defined_jobs[identifier].id = tracked_jobs[identifier].id
            dbt_cloud.update_job(job=defined_jobs[identifier])

    # Create new jobs
    logger.info("Detected {count} new jobs.", count=len(created_jobs))
    for identifier in created_jobs:
        dbt_cloud.create_job(job=defined_jobs[identifier])

    # Remove Deleted Jobs
    logger.warning("Detected {count} deleted jobs.", count=len(deleted_jobs))
    for identifier in deleted_jobs:
        dbt_cloud.delete_job(job=tracked_jobs[identifier])


if __name__ == "__main__":
    cli()
