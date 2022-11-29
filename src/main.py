import os
import sys

from loguru import logger
import click

from client import DBTCloud
from loader.load import load_job_configuration
from schemas import check_job_mapping_same


def compare_config_and_potentially_update(config, no_update):
    """Compares the config of YML files versus dbt Cloud.
    Depending on the value of no_update, it will either update the dbt Cloud config or not.

    CONFIG is the path to your jobs.yml config file.
    """
    configuration = load_job_configuration(config)
    defined_jobs = configuration.jobs

    # HACK for getting the account_id of one entry
    dbt_cloud = DBTCloud(
        account_id=list(defined_jobs.values())[0].account_id,
        api_key=os.environ.get("API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
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
            if no_update:
                logger.warning("-- Plan -- The job {identifier} is different and would be updated.", identifier=identifier)
            else:
                dbt_cloud.update_job(job=defined_jobs[identifier])

    # Create new jobs
    logger.info("Detected {count} new jobs.", count=len(created_jobs))
    for identifier in created_jobs:
        if no_update:
            logger.warning("-- Plan -- The job {identifier} is new and would be created.", identifier=identifier)
        else:
            dbt_cloud.create_job(job=defined_jobs[identifier])

    # Remove Deleted Jobs
    logger.warning("Detected {count} deleted jobs.", count=len(deleted_jobs))
    for identifier in deleted_jobs:
        if no_update:
            logger.warning("-- Plan -- The job {identifier} is deleted and would be removed.", identifier=identifier)
        else:
            dbt_cloud.delete_job(job=tracked_jobs[identifier])

    # -- ENV VARS --
    # Now that we have replicated all jobs we can get their IDs for further API calls
    mapping_job_identifier_job_id = dbt_cloud.build_mapping_job_identifier_job_id()
    logger.debug(f"Mapping of job identifier to id: {mapping_job_identifier_job_id}")

    # Replicate the env vars from the YML to dbt Cloud
    for job in defined_jobs.values():
        job_id = mapping_job_identifier_job_id[job.identifier]
        for env_var_yml in job.custom_environment_variables:
            env_var_yml.job_definition_id = job_id
            if no_update:
                logger.warning("-- Plan -- The env var {env_var} is new and would be created.", env_var=env_var_yml.name)
            else:
                updated_env_vars = dbt_cloud.update_env_var(
                    project_id=job.project_id, job_id=job_id, custom_env_var=env_var_yml
                )

    # Delete the env vars from dbt Cloud that are not in the yml
    for job in defined_jobs.values():
        job_id = mapping_job_identifier_job_id[job.identifier]

        # We get the env vars from dbt Cloud, now that the YML ones have been replicated
        env_var_dbt_cloud = dbt_cloud.get_env_vars(
            project_id=job.project_id, job_id=job_id
        )

        # And we get the list of env vars defined for a given job in the YML
        env_vars_for_job = [
            env_var.name for env_var in job.custom_environment_variables
        ]

        for env_var, env_var_val in env_var_dbt_cloud.items():
            # If the env var is not in the YML but is defined at the "job" level in dbt Cloud, we delete it
            if env_var not in env_vars_for_job and "job" in env_var_val:
                logger.info(f"{env_var} not in the YML file but in the dbt Cloud job")
                if no_update:
                    logger.warning("-- Plan -- The env var {env_var} is deleted and would be removed.", env_var=env_var)
                else:
                    dbt_cloud.delete_env_var(
                        project_id=job.project_id, env_var_id=env_var_val["job"]["id"]
                    )
                    logger.info(
                        f"Deleted the env_var {env_var} for the job {job.identifier}"
                    )

@click.group()
def cli():
    pass


@cli.command()
@click.argument("config", type=click.File("r"))
def sync(config):
    """Synchronize a dbt Cloud job config file against dbt Cloud.

    CONFIG is the path to your jobs.yml config file.
    """
    compare_config_and_potentially_update(config, no_update=False)


@cli.command()
@click.argument("config", type=click.File("r"))
def plan(config):
    """Check the difference betweeen a local file and dbt Cloud without updating dbt Cloud.

    CONFIG is the path to your jobs.yml config file.
    """
    compare_config_and_potentially_update(config, no_update=True)


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
    defined_jobs = load_job_configuration(config).jobs.values()

    if defined_jobs:
        logger.success("✅ The config file has a valid YML format.")

    if not online:
        return

    # Retrive the list of Project IDs and Environment IDs from the config file
    config_project_ids = set([job.project_id for job in defined_jobs])
    config_environment_ids = set([job.environment_id for job in defined_jobs])

    # Retrieve the list of Project IDs and Environment IDs from dbt Cloudby calling the environment API endpoint
    dbt_cloud = DBTCloud(
        account_id=list(defined_jobs)[0].account_id,
        api_key=os.environ.get("API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
    )
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
