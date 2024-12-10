import os
import sys
from importlib.metadata import version
from pathlib import Path

import click
from loguru import logger
from rich.console import Console

from dbt_jobs_as_code.client import DBTCloud, DBTCloudException
from dbt_jobs_as_code.cloud_yaml_mapping.change_set import build_change_set
from dbt_jobs_as_code.cloud_yaml_mapping.validate_link import LinkableCheck, can_be_linked
from dbt_jobs_as_code.exporter.export import export_jobs_yml
from dbt_jobs_as_code.importer import check_job_fields, fetch_jobs, get_account_id
from dbt_jobs_as_code.loader.load import load_job_configuration
from dbt_jobs_as_code.schemas.config import generate_config_schema

VERSION = version("dbt-jobs-as-code")

# adding the ability to disable ssl verification, useful for self-signed certificates and local testing
option_disable_ssl_verification = click.option(
    "--disable-ssl-verification",
    is_flag=True,
    envvar="DBT_JOBS_AS_CODE_DISABLE_SSL_VERIFICATION",
    show_envvar=True,
    default=False,
)

option_project_ids = click.option(
    "--project-id",
    "-p",
    type=int,
    multiple=True,
    help="[Optional] The ID of dbt Cloud project(s) to use for sync",
)

option_environment_ids = click.option(
    "--environment-id",
    "-e",
    type=int,
    multiple=True,
    help="[Optional] The ID of dbt Cloud environment(s) to use for sync",
)

option_limit_projects_envs_to_yml = click.option(
    "--limit-projects-envs-to-yml",
    "-l",
    is_flag=True,
    help="[Flag] Limit sync/plan to the projects and environments listed in the jobs YML file",
)

option_vars_yml = click.option(
    "--vars-yml",
    "-v",
    type=click.File("r"),
    help="The path to your vars_yml YML file when using a templated job YML file.",
)


@click.group(
    help=f"dbt-jobs-as-code {VERSION}\n\nA CLI to allow defining dbt Cloud jobs as code",
    context_settings={"max_content_width": 120},
)
@click.version_option(version=VERSION)
def cli() -> None:
    pass


@cli.command()
@option_disable_ssl_verification
@click.argument("config", type=click.File("r"))
@option_vars_yml
@option_project_ids
@option_environment_ids
@option_limit_projects_envs_to_yml
def sync(
    config,
    vars_yml,
    project_id,
    environment_id,
    limit_projects_envs_to_yml,
    disable_ssl_verification,
):
    """Synchronize a dbt Cloud job config file against dbt Cloud.

    CONFIG is the path to your jobs.yml config file.
    """
    cloud_project_ids = []
    cloud_environment_ids = []

    if limit_projects_envs_to_yml and (project_id or environment_id):
        logger.error(
            "You cannot use --limit-projects-envs-to-yml with --project-id or --environment-id. Please remove the --limit-projects-envs-to-yml flag."
        )
        sys.exit(1)

    if project_id:
        cloud_project_ids = list(project_id)

    if environment_id:
        cloud_environment_ids = list(environment_id)

    logger.info("-- SYNC -- Invoking build_change_set")
    change_set = build_change_set(
        config,
        vars_yml,
        disable_ssl_verification,
        cloud_project_ids,
        cloud_environment_ids,
        limit_projects_envs_to_yml,
    )
    if len(change_set) == 0:
        logger.success("-- SYNC -- No changes detected.")
    else:
        logger.info("-- SYNC -- {count} changes detected.", count=len(change_set))
        console = Console()
        console.log(change_set.to_table())
    change_set.apply()

    if not change_set.apply_success:
        logger.error("-- SYNC -- There were some errors during the sync. Check the logs.")
        sys.exit(1)


@cli.command()
@option_disable_ssl_verification
@click.argument("config", type=click.File("r"))
@option_vars_yml
@option_project_ids
@option_environment_ids
@option_limit_projects_envs_to_yml
def plan(
    config,
    vars_yml,
    project_id,
    environment_id,
    limit_projects_envs_to_yml,
    disable_ssl_verification,
):
    """Check the difference between a local file and dbt Cloud without updating dbt Cloud.

    CONFIG is the path to your jobs.yml config file.
    """
    cloud_project_ids = []
    cloud_environment_ids = []

    if limit_projects_envs_to_yml and (project_id or environment_id):
        logger.error(
            "You cannot use --limit-projects-envs-to-yml with --project-id or --environment-id. Please remove the --limit-projects-envs-to-yml flag."
        )
        sys.exit(1)

    if project_id:
        cloud_project_ids = list(project_id)

    if environment_id:
        cloud_environment_ids = list(environment_id)

    change_set = build_change_set(
        config,
        vars_yml,
        disable_ssl_verification,
        cloud_project_ids,
        cloud_environment_ids,
        limit_projects_envs_to_yml,
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
@option_vars_yml
@click.option("--online", is_flag=True, help="Connect to dbt Cloud to check that IDs are correct.")
def validate(config, vars_yml, online, disable_ssl_verification):
    """Check that the config file is valid

    CONFIG is the path to your jobs.yml config file.
    """

    # Parse the config file and check if it follows the Pydantic model
    logger.info(f"Parsing the YML file {config.name}")
    defined_jobs = load_job_configuration(config, vars_yml).jobs.values()

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
    all_environments = dbt_cloud.get_environments(project_ids=list(config_project_ids))
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
        project_ids = set([job.project_id for job in defined_jobs])
        cloud_jobs = dbt_cloud.get_jobs(project_ids=list(project_ids))
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
        list_project_ids = set([job.project_id for job in defined_jobs])
        cloud_envs = dbt_cloud.get_environments(project_ids=list(list_project_ids))
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
@option_project_ids
@option_environment_ids
@click.option(
    "--job-id",
    "-j",
    type=int,
    multiple=True,
    help="[Optional] The ID of the job to import.",
)
@click.option(
    "--check-missing-fields",
    is_flag=True,
    help="Check if the job model has missing fields.",
    hidden=True,
)
@click.option(
    "--include-linked-id",
    is_flag=True,
    help="Include the job ID when exporting jobs.",
)
def import_jobs(
    config,
    account_id,
    project_id,
    environment_id,
    job_id,
    disable_ssl_verification,
    check_missing_fields=False,
    include_linked_id=False,
):
    """
    Generate YML file for import.
    Either --config or --account-id must be provided.
    Optional parameters: --project-id,  --environment-id, --job-id
    It is possible to repeat the optional parameters --job-id, --project-id, --environment-id option to import specific jobs.
    """

    # we get the account id either from a parameter (e.g if the config file doesn't exist) or from the config file
    try:
        cloud_account_id = get_account_id(config, account_id)
    except ValueError as e:
        raise click.BadParameter(str(e)) from e

    dbt_cloud = DBTCloud(
        account_id=cloud_account_id,
        api_key=os.environ.get("DBT_API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
        disable_ssl_verification=disable_ssl_verification,
    )

    if check_missing_fields:
        check_job_fields(dbt_cloud, list(job_id))
        return

    cloud_jobs = fetch_jobs(dbt_cloud, list(job_id), list(project_id), list(environment_id))

    # Handle env vars
    for cloud_job in cloud_jobs:
        logger.info(f"Getting env vars overwrites for job {cloud_job.id}:{cloud_job.name}")
        env_vars = dbt_cloud.get_env_vars(
            project_id=cloud_job.project_id,
            job_id=cloud_job.id,  # type: ignore # in that case, we have an ID as we are importing
        )
        for env_var in env_vars.values():
            if env_var.value:
                cloud_job.custom_environment_variables.append(env_var)

    logger.success("YML file for the current dbt Cloud jobs")
    export_jobs_yml(cloud_jobs, include_linked_id)


@cli.command()
@option_disable_ssl_verification
@click.argument("config", type=click.File("r"))
@click.option("--dry-run", is_flag=True, help="In dry run mode we don't update dbt Cloud.")
def link(config, dry_run, disable_ssl_verification):
    """
    Link the YML file to dbt Cloud by adding the identifier to the job name.
    All relevant jobs get the part [[...]] added to their name
    """

    yaml_jobs = load_job_configuration(config, None).jobs
    account_id = list(yaml_jobs.values())[0].account_id

    dbt_cloud = DBTCloud(
        account_id=account_id,
        api_key=os.environ.get("DBT_API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
        disable_ssl_verification=disable_ssl_verification,
    )

    some_jobs_updated = False
    for current_identifier, job_details in yaml_jobs.items():
        linkable_check = can_be_linked(current_identifier, job_details, dbt_cloud)
        if not linkable_check.can_be_linked:
            logger.error(linkable_check.message)
            continue

        # impossible according to the check but needed to fix type checking
        assert linkable_check.linked_job is not None

        cloud_job = linkable_check.linked_job
        cloud_job.identifier = current_identifier
        if dry_run:
            logger.info(
                f"Would link/rename the job {cloud_job.id}:{cloud_job.name} [[{current_identifier}]]"
            )
        else:
            logger.info(
                f"Linking/Renaming the job {cloud_job.id}:{cloud_job.name} [[{current_identifier}]]"
            )
            dbt_cloud.update_job(job=cloud_job)
            some_jobs_updated = True

    if not dry_run:
        if some_jobs_updated:
            logger.success(f"Updated all jobs!")
        else:
            logger.info(f"No jobs to link")


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
        # we get the account id from the config file
        defined_jobs = load_job_configuration(config, None).jobs.values()
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
                f"Would link/rename the job {cloud_job.id}:{cloud_job.name} [[{current_identifier}]]"
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
    help="The ID of the job to deactivate.",
)
def deactivate_jobs(config, account_id, job_id, disable_ssl_verification):
    """
    Deactivate jobs triggers in dbt Cloud (schedule and CI/CI triggers)
    """

    # we get the account id either from a parameter (e.g if the config file doesn't exist) or from the config file
    if account_id:
        cloud_account_id = account_id
    elif config:
        defined_jobs = load_job_configuration(config, None).jobs.values()
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
            or cloud_job.triggers.on_merge
        ):
            logger.info(f"Deactivating the job {cloud_job.id}:{cloud_job.name}")
            cloud_job.triggers.github_webhook = False
            cloud_job.triggers.git_provider_webhook = False
            cloud_job.triggers.schedule = False
            cloud_job.triggers.on_merge = False
            dbt_cloud.update_job(job=cloud_job)
        else:
            logger.info(f"The job {cloud_job.id}:{cloud_job.name} is already deactivated")

    logger.success(f"Deactivated all jobs!")


@cli.command(hidden=True)
def update_json_schema():
    json_schema = generate_config_schema()
    Path("src/dbt_jobs_as_code/schemas/load_job_schema.json").write_text(json_schema)


if __name__ == "__main__":
    cli()
