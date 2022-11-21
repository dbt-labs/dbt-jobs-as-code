import os
import sys

from loguru import logger
import click

from client import DBTCloud
from loader.load import load_job_definitions
from schemas import check_job_mapping_same


@click.group()
def cli():
    pass


@cli.command()
@click.argument("config", type=click.File("r"))
def sync(config):
    """Synchronize a dbt Cloud job config file against dbt Cloud.

    CONFIG is the path to your jobs.yml config file.
    """
    defined_jobs = load_job_definitions(config)

    dbt_cloud = DBTCloud(account_id=43791, api_key=os.environ.get("API_KEY"))
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


@cli.command()
@click.argument("config", type=click.File("r"))
@click.option(
    "--online", is_flag=True, help="Connect to dbt Cloud to check that IDs are correct."
)
def validate(config, online):
    """Check that the config file is valid

    CONFIG is the path to your jobs.yml config file.
    """

    # Parse the config file and check if it follows the Pydantic model
    logger.info(f"Parsing the YML file {config.name}")
    defined_jobs = load_job_definitions(config).values()

    if defined_jobs:
        logger.success("✅ The config file has a valid YML format.")

    if not online:
        return

    # Retrive the list of Project IDs and Environment IDs from the config file
    config_project_ids = set([job.project_id for job in defined_jobs])
    config_environment_ids = set([job.environment_id for job in defined_jobs])

    # Retrieve the list of Project IDs and Environment IDs from dbt Cloudby calling the environment API endpoint
    dbt_cloud = DBTCloud(account_id=43791, api_key=os.environ.get("API_KEY"))
    all_environments = dbt_cloud.get_environments()
    cloud_project_ids = set([env["project_id"] for env in all_environments])
    cloud_environment_ids = set([env["id"] for env in all_environments])

    online_check_issues = False

    # Check if some Project IDs in the config are not in Cloud
    logger.info(f"Checking that Project IDs are valid")
    if config_project_ids - cloud_project_ids:
        logger.error(
            f"❌ The following project IDs are not valid: {config_project_ids - cloud_project_ids}"
        )
        online_check_issues = True

    # Check if some Environment IDs in the config are not in Cloud
    logger.info(f"Checking that Environments IDs are valid")
    if config_environment_ids - cloud_environment_ids:
        logger.error(
            f"❌ The following environment IDs are not valid: {config_environment_ids - cloud_environment_ids}"
        )
        online_check_issues = True

    # In case deferral jobs are mentioned, check that they exist
    deferral_jobs = set(
        [
            job.deferring_job_definition_id
            for job in defined_jobs
            if job.deferring_job_definition_id
        ]
    )
    if deferral_jobs:
        logger.info(f"Checking that Deferring Job IDs are valid")
        cloud_jobs = dbt_cloud.get_jobs()
        cloud_job_ids = set([job.id for job in cloud_jobs])
        if deferral_jobs - cloud_job_ids:
            logger.error(
                f"❌ The following deferral job IDs are not valid: {deferral_jobs - cloud_job_ids}"
            )
            online_check_issues = True

    if online_check_issues:
        # return an error to handle with bash/CI
        sys.exit(1)

    logger.success("✅ The config file is valid")


if __name__ == "__main__":
    cli()
