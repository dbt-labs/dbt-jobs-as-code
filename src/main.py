import os
import sys

from loguru import logger
import click

from src.client import DBTCloud
from src.loader.load import load_job_configuration
from src.exporter.export import export_jobs_yml
from src.schemas import check_job_mapping_same
from src.changeset.change_set import Change, ChangeSet
from src.schemas import check_env_var_same
from rich.console import Console

# adding the ability to disable ssl verification, useful for self-signed certificates and local testing
option_disable_ssl_verification = click.option(
    "--disable-ssl-verification",
    is_flag=True,
    envvar="DBT_JOBS_AS_CODE_DISABLE_SSL_VERIFICATION",
    show_envvar=True,
    default=False,
)


def filter_config(defined_jobs, project_id, environment_id):
    removed_job_ids = set()
    if len(environment_id) != 0:
        for job_id, job in defined_jobs.items():
            if not job.environment_id in environment_id:
                removed_job_ids.add(job_id)
                logger.warning(
                    f"For Job# {job.identifier}, environment_id(s) provided as arguments does not match the ID's in Jobs YAML file!!!"
                )
    if len(project_id) != 0:
        for job_id, job in defined_jobs.items():
            if not job.project_id in project_id:
                removed_job_ids.add(job_id)
                logger.warning(
                    f"For Job# {job.identifier}, project_id(s) provided as arguments does not match the ID's in Jobs YAML file!!!"
                )
    return {job_id: job for job_id, job in defined_jobs.items() if job_id not in removed_job_ids}


def build_change_set(config, disable_ssl_verification, project_id, environment_id):
    """Compares the config of YML files versus dbt Cloud.
    Depending on the value of no_update, it will either update the dbt Cloud config or not.

    CONFIG is the path to your jobs.yml config file.
    """
    configuration = load_job_configuration(config)
    unfiltered_defined_jobs = configuration.jobs

    # If a project_id or environment_id is passed in as a parameter (one or multiple), check if these match the ID's in Jobs YAML file, otherwise add a warning and continue the process
    defined_jobs = filter_config(unfiltered_defined_jobs, project_id, environment_id)

    if len(defined_jobs) == 0:
        logger.error(
            "No jobs found in the Jobs YAML file after filtering based on the project_id and environment_id provided as arguments!!!"
        )
        return ChangeSet()

    # HACK for getting the account_id of one entry
    dbt_cloud = DBTCloud(
        account_id=list(defined_jobs.values())[0].account_id,
        api_key=os.environ.get("DBT_API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
        disable_ssl_verification=disable_ssl_verification,
    )

    cloud_jobs = dbt_cloud.get_jobs(project_ids=project_id, environment_ids=environment_id)
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
                proj_id=defined_jobs[identifier].project_id,
                env_id=defined_jobs[identifier].environment_id,
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
            proj_id=defined_jobs[identifier].project_id,
            env_id=defined_jobs[identifier].environment_id,
            sync_function=dbt_cloud.create_job,
            parameters={"job": defined_jobs[identifier]},
        )
        dbt_cloud_change_set.append(dbt_cloud_change)

    # Remove Deleted Jobs
    logger.info("Detected {count} deleted jobs.", count=len(deleted_jobs))
    for identifier in deleted_jobs:
        dbt_cloud_change = Change(
            identifier=identifier,
            type="job",
            action="delete",
            proj_id=tracked_jobs[identifier].project_id,
            env_id=tracked_jobs[identifier].environment_id,
            sync_function=dbt_cloud.delete_job,
            parameters={"job": tracked_jobs[identifier]},
        )
        dbt_cloud_change_set.append(dbt_cloud_change)

    # -- ENV VARS --
    # Now that we have replicated all jobs we can get their IDs for further API calls
    mapping_job_identifier_job_id = dbt_cloud.build_mapping_job_identifier_job_id(cloud_jobs)
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
                        proj_id=job.project_id,
                        env_id=job.environment_id,
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
                    proj_id=job.project_id,
                    env_id=job.environment_id,
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
                        proj_id=job.project_id,
                        env_id=job.environment_id,
                        sync_function=dbt_cloud.delete_env_var,
                        parameters={
                            "project_id": job.project_id,
                            "env_var_id": env_var_val.id,
                        },
                    )
                    dbt_cloud_change_set.append(dbt_cloud_change)

    # Filtering out the change set, if project_id(s), environment_id(s) are passed as arguments to function
    # TODO: Confirm if this is the desired functionality, remove otherwise
    logger.debug(f"dbt cloud change set: {dbt_cloud_change_set}")

    return dbt_cloud_change_set


@click.group()
def cli() -> None:
    pass


@cli.command()
@option_disable_ssl_verification
@click.argument("config", type=click.File("r"))
@click.option(
    "--project-id",
    "-p",
    type=int,
    multiple=True,
    help="[Optional] The ID of dbt Cloud project(s) to use for sync",
)
@click.option(
    "--environment-id",
    "-e",
    type=int,
    multiple=True,
    help="[Optional] The ID of dbt Cloud environment(s) to use for sync",
)
def sync(config, project_id, environment_id, disable_ssl_verification):
    """Synchronize a dbt Cloud job config file against dbt Cloud.

    CONFIG is the path to your jobs.yml config file.
    """
    cloud_project_id = []
    cloud_environment_id = []

    if project_id:
        cloud_project_id = project_id

    if environment_id:
        cloud_environment_id = environment_id

    logger.info("-- SYNC -- Invoking build_change_set")
    change_set = build_change_set(
        config, disable_ssl_verification, cloud_project_id, cloud_environment_id
    )
    if len(change_set) == 0:
        logger.success("-- SYNC -- No changes detected.")
    else:
        logger.info("-- SYNC -- {count} changes detected.", count=len(change_set))
        console = Console()
        console.log(change_set.to_table())
    change_set.apply()


@cli.command()
@option_disable_ssl_verification
@click.argument("config", type=click.File("r"))
@click.option(
    "--project-id",
    "-p",
    type=int,
    multiple=True,
    help="[Optional] The ID of dbt Cloud project(s) to use for plan",
)
@click.option(
    "--environment-id",
    "-e",
    type=int,
    multiple=True,
    help="[Optional] The ID of dbt Cloud environment(s) to use for plan",
)
def plan(config, project_id, environment_id, disable_ssl_verification):
    """Check the difference between a local file and dbt Cloud without updating dbt Cloud.

    CONFIG is the path to your jobs.yml config file.
    """
    cloud_project_id = []
    cloud_environment_id = []

    if project_id:
        cloud_project_id = project_id

    if environment_id:
        cloud_environment_id = environment_id

    change_set = build_change_set(
        config, disable_ssl_verification, cloud_project_id, cloud_environment_id
    )
    if len(change_set) == 0:
        logger.success("-- PLAN -- No changes detected.")
    else:
        logger.info("-- PLAN -- {count} changes detected.", count=len(change_set))
        console = Console()
        console.log(change_set.to_table())


@cli.command()
@option_disable_ssl_verification
@click.argument("config", type=click.File("r"))
@click.option("--online", is_flag=True, help="Connect to dbt Cloud to check that IDs are correct.")
def validate(config, online, disable_ssl_verification):
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

    # Retrieve the list of Project IDs and Environment IDs from the config file
    config_project_ids = set([job.project_id for job in defined_jobs])
    config_environment_ids = set([job.environment_id for job in defined_jobs])

    # Retrieve the list of Project IDs and Environment IDs from dbt Cloud by calling the environment API endpoint
    dbt_cloud = DBTCloud(
        account_id=list(defined_jobs)[0].account_id,
        api_key=os.environ.get("DBT_API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
        disable_ssl_verification=disable_ssl_verification,
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

    # In case deferral jobs are mentioned, check that they exist
    deferral_envs = set(
        [job.deferring_environment_id for job in defined_jobs if job.deferring_environment_id]
    )
    if deferral_envs:
        logger.info(f"Checking that Deferring Env IDs are valid")
        cloud_envs = dbt_cloud.get_environments()
        cloud_envs_ids = set([env["id"] for env in cloud_envs])
        if deferral_envs - cloud_envs_ids:
            logger.error(
                f"❌ The following deferral environment IDs are not valid: {deferral_envs - cloud_envs_ids}"
            )
            online_check_issues = True

    if online_check_issues:
        # return an error to handle with bash/CI
        sys.exit(1)

    logger.success("✅ The config file is valid")


@cli.command()
@option_disable_ssl_verification
@click.option("--config", type=click.File("r"), help="The path to your YML jobs config file.")
@click.option("--account-id", type=int, help="The ID of your dbt Cloud account.")
@click.option(
    "--project-id",
    "-p",
    type=int,
    multiple=True,
    help="[Optional] The ID of dbt Cloud project(s) to use for import",
)
@click.option(
    "--environment-id",
    "-e",
    type=int,
    multiple=True,
    help="[Optional] The ID of dbt Cloud environment(s) to use for import",
)
@click.option(
    "--job-id",
    "-j",
    type=int,
    multiple=True,
    help="[Optional] The ID of the job to import.",
)
def import_jobs(config, account_id, project_id, environment_id, job_id, disable_ssl_verification):
    """
    Generate YML file for import.
    Either --config or --account-id must be provided.
    Optional parameters: --project-id,  --environment-id, --job-id
    It is possible to repeat the optional parameters --job-id, --project-id, --environment-id option to import specific jobs.
    """

    # we get the account id either from a parameter (e.g if the config file doesn't exist) or from the config file
    if account_id:
        cloud_account_id = account_id
    elif config:
        defined_jobs = load_job_configuration(config).jobs.values()
        cloud_account_id = list(defined_jobs)[0].account_id
    else:
        raise click.BadParameter("Either --config or --account-id must be provided")

    cloud_project_id = []
    cloud_environment_id = []

    if project_id:
        cloud_project_id = project_id

    if environment_id:
        cloud_environment_id = environment_id

    dbt_cloud = DBTCloud(
        account_id=cloud_account_id,
        api_key=os.environ.get("DBT_API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
        disable_ssl_verification=disable_ssl_verification,
    )
    cloud_jobs = dbt_cloud.get_jobs(
        project_ids=cloud_project_id, environment_ids=cloud_environment_id
    )
    logger.info(f"Getting the jobs definition from dbt Cloud")

    if job_id:
        cloud_jobs = [job for job in cloud_jobs if job.id in job_id]

    for cloud_job in cloud_jobs:
        logger.info(f"Getting en vars overwrites for the job {cloud_job.id}:{cloud_job.name}")
        env_vars = dbt_cloud.get_env_vars(project_id=cloud_job.project_id, job_id=cloud_job.id)
        for env_var in env_vars.values():
            if env_var.value:
                cloud_job.custom_environment_variables.append(env_var)

    logger.success(f"YML file for the current dbt Cloud jobs")
    export_jobs_yml(cloud_jobs)


@cli.command()
@option_disable_ssl_verification
@click.option("--config", type=click.File("r"), help="The path to your YML jobs config file.")
@click.option("--account-id", type=int, help="The ID of your dbt Cloud account.")
@click.option("--dry-run", is_flag=True, help="In dry run mode we don't update dbt Cloud.")
@click.option(
    "--identifier",
    "-i",
    type=str,
    multiple=True,
    help="[Optional] The identifiers we want to unlink. If not provided, all jobs are unlinked.",
)
def unlink(config, account_id, dry_run, identifier, disable_ssl_verification):
    """
    Unlink the YML file to dbt Cloud.
    All relevant jobs get the part [[...]] removed from their name
    """

    # we get the account id either from a parameter (e.g if the config file doesn't exist) or from the config file
    if account_id:
        cloud_account_id = account_id
    elif config:
        defined_jobs = load_job_configuration(config).jobs.values()
        cloud_account_id = list(defined_jobs)[0].account_id
    else:
        raise click.BadParameter("Either --config or --account-id must be provided")

    # we get the account id from the config file
    defined_jobs = load_job_configuration(config).jobs.values()
    cloud_account_id = list(defined_jobs)[0].account_id

    dbt_cloud = DBTCloud(
        account_id=cloud_account_id,
        api_key=os.environ.get("DBT_API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
        disable_ssl_verification=disable_ssl_verification,
    )
    cloud_jobs = dbt_cloud.get_jobs()
    selected_jobs = [job for job in cloud_jobs if job.identifier is not None]
    logger.info(f"Getting the jobs definition from dbt Cloud")

    if identifier:
        selected_jobs = [job for job in selected_jobs if job.identifier in identifier]

    for cloud_job in selected_jobs:
        current_identifier = cloud_job.identifier
        # by removing the identifier, we unlink the job from the YML file
        cloud_job.identifier = None
        if dry_run:
            logger.info(
                f"Would unlink/rename the job {cloud_job.id}:{cloud_job.name} [[{current_identifier}]]"
            )
        else:
            logger.info(
                f"Unlinking/Renaming the job {cloud_job.id}:{cloud_job.name} [[{current_identifier}]]"
            )
            dbt_cloud.update_job(job=cloud_job)

    if len(selected_jobs) == 0:
        logger.info(f"No jobs to unlink")
    elif not dry_run:
        logger.success(f"Updated all jobs!")


@cli.command()
@option_disable_ssl_verification
@click.option("--config", type=click.File("r"), help="The path to your YML jobs config file.")
@click.option("--account-id", type=int, help="The ID of your dbt Cloud account.")
@click.option(
    "--job-id",
    "-j",
    type=int,
    multiple=True,
    help="[Optional] The ID of the job to deactivate.",
)
def deactivate_jobs(config, account_id, job_id, disable_ssl_verification):
    """
    Deactivate jobs triggers in dbt Cloud (schedule and CI/CI triggers)
    """

    # we get the account id either from a parameter (e.g if the config file doesn't exist) or from the config file
    if account_id:
        cloud_account_id = account_id
    elif config:
        defined_jobs = load_job_configuration(config).jobs.values()
        cloud_account_id = list(defined_jobs)[0].account_id
    else:
        raise click.BadParameter("Either --config or --account-id must be provided")

    dbt_cloud = DBTCloud(
        account_id=cloud_account_id,
        api_key=os.environ.get("DBT_API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
        disable_ssl_verification=disable_ssl_verification,
    )
    cloud_jobs = dbt_cloud.get_jobs()

    selected_cloud_jobs = [job for job in cloud_jobs if job.id in job_id]

    for cloud_job in selected_cloud_jobs:
        if (
            cloud_job.triggers.git_provider_webhook
            or cloud_job.triggers.github_webhook
            or cloud_job.triggers.schedule
        ):
            logger.info(f"Deactivating the job {cloud_job.id}:{cloud_job.name}")
            cloud_job.triggers.github_webhook = False
            cloud_job.triggers.git_provider_webhook = False
            cloud_job.triggers.schedule = False
            dbt_cloud.update_job(job=cloud_job)
        else:
            logger.info(f"The job {cloud_job.id}:{cloud_job.name} is already deactivated")

    logger.success(f"Deactivated all jobs!")


if __name__ == "__main__":
    cli()
