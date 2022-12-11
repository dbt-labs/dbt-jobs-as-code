import os
from ruamel.yaml import YAML
import sys

from loguru import logger
import click
import asyncio

from client import DBTCloud
from loader.load import load_job_configuration
from exporter.export import export_jobs_yml
from schemas import check_job_mapping_same
from changeset.change_set import Change, ChangeSet
from schemas import check_env_var_same


def build_change_set(config):
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
    tracked_jobs = {job.identifier: job for job in cloud_jobs if job.identifier is not None}

    dbt_cloud_change_set = ChangeSet()

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
            dbt_cloud_change = Change(
                identifier=identifier,
                type="job",
                action="update",
                sync_function=dbt_cloud.update_job,
                parameters={"job": defined_jobs[identifier]},
            )
            dbt_cloud_change_set.append(dbt_cloud_change)
            defined_jobs[identifier].id = tracked_jobs[identifier].id

    # Create new jobs
    logger.info("Detected {count} new jobs.", count=len(created_jobs))
    for identifier in created_jobs:
        dbt_cloud_change = Change(
            identifier=identifier,
            type="job",
            action="create",
            sync_function=dbt_cloud.create_job,
            parameters={"job": defined_jobs[identifier]},
        )
        dbt_cloud_change_set.append(dbt_cloud_change)

    # Remove Deleted Jobs
    logger.warning("Detected {count} deleted jobs.", count=len(deleted_jobs))
    for identifier in deleted_jobs:
        dbt_cloud_change = Change(
            identifier=identifier,
            type="job",
            action="delete",
            sync_function=dbt_cloud.delete_job,
            parameters={"job": tracked_jobs[identifier]},
        )
        dbt_cloud_change_set.append(dbt_cloud_change)

    # -- ENV VARS --
    # Now that we have replicated all jobs we can get their IDs for further API calls
    mapping_job_identifier_job_id = dbt_cloud.build_mapping_job_identifier_job_id()
    logger.debug(f"Mapping of job identifier to id: {mapping_job_identifier_job_id}")

    # Replicate the env vars from the YML to dbt Cloud
    for job in defined_jobs.values():

        if job.identifier in mapping_job_identifier_job_id:  # the job already exists
            job_id = mapping_job_identifier_job_id[job.identifier]
            all_env_vars_for_job = dbt_cloud.get_env_vars(project_id=job.project_id, job_id=job_id)
            for env_var_yml in job.custom_environment_variables:
                env_var_yml.job_definition_id = job_id
                same_env_var, env_var_id = check_env_var_same(
                    source_env_var=env_var_yml, dest_env_vars=all_env_vars_for_job
                )
                if not same_env_var:
                    dbt_cloud_change = Change(
                        identifier=f"{job.identifier}:{env_var_yml.name}",
                        type="env var overwrite",
                        action="update",
                        sync_function=dbt_cloud.update_env_var,
                        parameters={
                            "project_id": job.project_id,
                            "job_id": job_id,
                            "custom_env_var": env_var_yml,
                            "env_var_id": env_var_id,
                        },
                    )
                    dbt_cloud_change_set.append(dbt_cloud_change)

        else:  # the job doesn't exist yet so it doesn't have an ID
            for env_var_yml in job.custom_environment_variables:
                dbt_cloud_change = Change(
                    identifier=f"{job.identifier}:{env_var_yml.name}",
                    type="env var overwrite",
                    action="create",
                    sync_function=dbt_cloud.update_env_var,
                    parameters={
                        "project_id": job.project_id,
                        "job_id": None,
                        "custom_env_var": env_var_yml,
                        "env_var_id": None,
                        "yml_job_identifier": job.identifier,
                    },
                )
                dbt_cloud_change_set.append(dbt_cloud_change)

    # Delete the env vars from dbt Cloud that are not in the yml
    for job in defined_jobs.values():

        # we only delete env var overwrite if the job already exists
        if job.identifier in mapping_job_identifier_job_id:
            job_id = mapping_job_identifier_job_id[job.identifier]

            # We get the env vars from dbt Cloud, now that the YML ones have been replicated
            env_var_dbt_cloud = dbt_cloud.get_env_vars(project_id=job.project_id, job_id=job_id)

            # And we get the list of env vars defined for a given job in the YML
            env_vars_for_job = [env_var.name for env_var in job.custom_environment_variables]

            for env_var, env_var_val in env_var_dbt_cloud.items():
                # If the env var is not in the YML but is defined at the "job" level in dbt Cloud, we delete it
                if env_var not in env_vars_for_job and env_var_val.id:
                    logger.info(f"{env_var} not in the YML file but in the dbt Cloud job")
                    dbt_cloud_change = Change(
                        identifier=f"{job.identifier}:{env_var_yml.name}",
                        type="env var overwrite",
                        action="delete",
                        sync_function=dbt_cloud.delete_env_var,
                        parameters={
                            "project_id": job.project_id,
                            "env_var_id": env_var_val.id,
                        },
                    )
                    dbt_cloud_change_set.append(dbt_cloud_change)

    return dbt_cloud_change_set


@click.group()
def cli():
    pass


@cli.command()
@click.argument("config", type=click.File("r"))
def sync(config):
    """Synchronize a dbt Cloud job config file against dbt Cloud.

    CONFIG is the path to your jobs.yml config file.
    """
    change_set = build_change_set(config)
    if len(change_set) == 0:
        logger.success("-- PLAN -- No changes detected.")
    else:
        logger.warning("-- PLAN -- {count} changes detected.", count=len(change_set))
        print(change_set)
    logger.info("-- SYNC --")
    change_set.apply()


@cli.command()
@click.argument("config", type=click.File("r"))
def plan(config):
    """Check the difference between a local file and dbt Cloud without updating dbt Cloud.

    CONFIG is the path to your jobs.yml config file.
    """
    change_set = build_change_set(config)
    if len(change_set) == 0:
        logger.success("-- PLAN -- No changes detected.")
    else:
        logger.warning("-- PLAN -- {count} changes detected.", count=len(change_set))
        print(change_set)


@cli.command()
@click.argument("config", type=click.File("r"))
@click.option("--online", is_flag=True, help="Connect to dbt Cloud to check that IDs are correct.")
def validate(config, online):
    """Check that the config file is valid

    CONFIG is the path to your jobs.yml config file.
    """

    # Parse the config file and check if it follows the Pydantic model
    logger.info(f"Parsing the YML file {config.name}")
    defined_jobs = load_job_configuration(config).jobs.values()

    for job in defined_jobs:
        print(job.to_load_format())

    if defined_jobs:
        logger.success("✅ The config file has a valid YML format.")

    if not online:
        return

    # Retrieve the list of Project IDs and Environment IDs from the config file
    config_project_ids = set([job.project_id for job in defined_jobs])
    config_environment_ids = set([job.environment_id for job in defined_jobs])

    # Retrieve the list of Project IDs and Environment IDs from dbt Cloud by calling the environment API endpoint
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


@cli.command()
@click.option("--config", type=click.File("r"), help="The path to your YML jobs config file.")
@click.option("--account-id", type=int, help="The ID of your dbt Cloud account.")
@click.option(
    "--job-id",
    "-j",
    type=int,
    multiple=True,
    help="[Optional] The ID of the job to import.",
)
def import_jobs(config, account_id, job_id):
    """
    Generate YML file for import.
    Either --config or --account-id must be provided.

    It is possible to repeat the optional --job-id option to import specific jobs.
    """

    # we get the account id either from a parameter (e.g if the config file doesn't exist) or from the config file
    if account_id:
        cloud_account_id = account_id
    elif config:
        defined_jobs = load_job_configuration(config).jobs.values()
        cloud_account_id = list(defined_jobs)[0].account_id
    else:
        raise Exception("Either --config or --account-id must be provided")

    dbt_cloud = DBTCloud(
        account_id=cloud_account_id,
        api_key=os.environ.get("API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
    )
    cloud_jobs = dbt_cloud.get_jobs()
    logger.info(f"Getting the jobs definition from dbt Cloud")

    if job_id:
        cloud_jobs = [job for job in cloud_jobs if job.id in job_id]

    vals = asyncio.run(dbt_cloud.get_env_vars_for_jobs(cloud_jobs))

    for env_var_dict in vals:
        env_var = list(env_var_dict.values())[0]
        print(env_var)
        if env_var.value:
            cloud_job = [job for job in cloud_jobs if job.id == env_var.job_definition_id][0]
            cloud_job.custom_environment_variables.append(env_var)

    logger.success(f"YML file for the current dbt Cloud jobs")
    export_jobs_yml(cloud_jobs)


if __name__ == "__main__":
    cli()
