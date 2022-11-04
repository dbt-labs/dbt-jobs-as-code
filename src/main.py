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

    # HACK for getting the account_id of one entry
    dbt_cloud = DBTCloud(
        account_id=list(defined_jobs.values())[0].account_id, api_key=os.environ.get("API_KEY"), base_url=os.environ.get("DBT_BASE_URL")
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

    # -- ENV VARS --
    # Now that we have replicated all jobs we can get their IDs for further API calls
    mapping_job_identifier_job_id = dbt_cloud.build_mapping_job_identifier_job_id()
    logger.debug(f"Mapping of job identifier to id: {mapping_job_identifier_job_id}")

    # Replicate the env vars from the YML to dbt Cloud
    for job in defined_jobs.values():
        job_id = mapping_job_identifier_job_id[job.identifier]
        for env_var_yml in job.custom_environment_variables:
            updated_env_vars = dbt_cloud.update_env_var(project_id=job.project_id, job_id=job_id, custom_env_var=env_var_yml)

    # Delete the env vars from dbt Cloud that are not in the yml
    for job in defined_jobs.values():
        job_id = mapping_job_identifier_job_id[job.identifier]

        # We get the env vars from dbt Cloud, now that the YML ones have been replicated
        env_var_dbt_cloud = dbt_cloud.get_env_vars(project_id=job.project_id, job_id=job_id)

        # And we get the list of env vars defined for a given job in the YML
        env_vars_for_job = [env_var.name for env_var in job.custom_environment_variables]

        for env_var, env_var_val in env_var_dbt_cloud.items():
            # If the env var is not in the YML but is defined at the "job" level in dbt Cloud, we delete it
            if env_var not in env_vars_for_job and "job" in env_var_val :
                logger.info(f"{env_var} not in the YML file but in the dbt Cloud job")
                dbt_cloud.delete_env_var(project_id=job.project_id, env_var_id=env_var_val["job"]["id"])
                logger.info(f"Deleted the env_var {env_var} for the job {job.identifier}")

if __name__ == "__main__":
    cli()
