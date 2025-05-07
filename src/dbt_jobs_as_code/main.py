import json
import os
import sys
from importlib.metadata import version
from pathlib import Path
from typing import List

import click
from loguru import logger
from rich.console import Console
from ruamel.yaml import YAML

from dbt_jobs_as_code.client import DBTCloud
from dbt_jobs_as_code.cloud_yaml_mapping.change_set import build_change_set, json_serializer_type
from dbt_jobs_as_code.cloud_yaml_mapping.validate_link import can_be_linked
from dbt_jobs_as_code.exporter.export import export_jobs_yml
from dbt_jobs_as_code.importer import check_job_fields, fetch_jobs, get_account_id
from dbt_jobs_as_code.loader.load import load_job_configuration, resolve_file_paths
from dbt_jobs_as_code.schemas.config import generate_config_schema
from dbt_jobs_as_code.schemas.job import filter_jobs_by_import_filter

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
    type=str,
    help="The path to your vars_yml YML file (or pattern for those files) when using a templated job YML file.",
)

option_json_output = click.option(
    "--json",
    "output_json",
    is_flag=True,
    help="Output results in JSON format instead of human-readable text.",
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
@click.argument("config", type=str)
@option_vars_yml
@option_project_ids
@option_environment_ids
@option_limit_projects_envs_to_yml
@option_json_output
def sync(
    config: str,
    vars_yml,
    project_id,
    environment_id,
    limit_projects_envs_to_yml,
    disable_ssl_verification,
    output_json: bool,
):
    """Synchronize a dbt Cloud job config file against dbt Cloud.
    This command will update dbt Cloud with the changes in the local YML file. It is recommended to run a `plan` first to see what will be changed.

    CONFIG is the path to your YML jobs config file (also supports glob patterns for those files or a directory).
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
        output_json=output_json,
    )
    if len(change_set) == 0:
        if output_json:
            print(json.dumps({"job_changes": [], "env_var_overwrite_changes": []}))
        else:
            logger.success("-- SYNC -- No changes detected.")
    else:
        if output_json:
            print(json.dumps(change_set.to_json()))
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
@click.argument("config", type=str)
@option_vars_yml
@option_project_ids
@option_environment_ids
@option_limit_projects_envs_to_yml
@option_json_output
def plan(
    config: str,
    vars_yml: str,
    project_id: List[int],
    environment_id: List[int],
    limit_projects_envs_to_yml: bool,
    disable_ssl_verification: bool,
    output_json: bool,
):
    """Check the difference between a local file and dbt Cloud without updating dbt Cloud.
    This command will not update dbt Cloud.

    CONFIG is the path to your YML jobs config file (also supports glob patterns for those files or a directory).
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
        output_json=output_json,
    )
    if len(change_set) == 0:
        if output_json:
            print(json.dumps({"job_changes": [], "env_var_overwrite_changes": []}))
        else:
            logger.success("-- PLAN -- No changes detected.")
    else:
        if output_json:
            print(json.dumps(change_set.to_json(), default=json_serializer_type))
        else:
            logger.info("-- PLAN -- {count} changes detected.", count=len(change_set))
            console = Console()
            console.log(change_set.to_table())


@cli.command()
@option_disable_ssl_verification
@click.argument("config", type=str)
@option_vars_yml
@click.option("--online", is_flag=True, help="Connect to dbt Cloud to check that IDs are correct.")
def validate(config, vars_yml, online, disable_ssl_verification):
    """Check that the config file is valid

    CONFIG is the path to your YML jobs config file (also supports glob patterns for those files or a directory).
    """
    try:
        config_files, vars_files = resolve_file_paths(config, vars_yml)
        defined_jobs = load_job_configuration(config_files, vars_files).jobs.values()

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
        logger.info("Checking that Project IDs are valid")
        if config_project_ids - cloud_project_ids:
            logger.error(
                f"❌ The following project IDs are not valid: {config_project_ids - cloud_project_ids}"
            )
            online_check_issues = True

        # Check if some Environment IDs in the config are not in Cloud
        logger.info("Checking that Environments IDs are valid")
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
            logger.info("Checking that Deferring Job IDs are valid")
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
            logger.info("Checking that Deferring Env IDs are valid")
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
    except ValueError as e:
        logger.error(f"Error validating config: {e}")
        sys.exit(1)


@cli.command()
@option_disable_ssl_verification
@click.option(
    "--config",
    type=str,
    help="The path to your YML jobs config file (also supports glob patterns for those files or a directory).",
)
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
@click.option(
    "--managed-only",
    is_flag=True,
    help="Only import jobs that are managed (have an identifier).",
)
@click.option(
    "--templated-fields",
    type=str,
    help="Path to a YAML file containing field templates to apply to the exported jobs.",
)
@click.option(
    "--filter",
    type=str,
    help="Only import jobs where the identifier prefix, before `:` contains this value, is empty or is '*'.",
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
    managed_only=False,
    templated_fields=None,
    filter=None,
):
    """
    Generate YML file for import.

    Either --config or --account-id must be provided to mention what Account ID to use.

    Optional parameters: --project-id,  --environment-id, --job-id

    It is possible to repeat the optional parameters --job-id, --project-id, --environment-id option to import specific jobs.
    """
    try:
        # Validate templated_fields file if provided
        if templated_fields:
            try:
                yaml = YAML()
                with open(templated_fields, "r") as f:
                    yaml.load(f)
            except Exception as e:
                raise ValueError(f"Invalid templated fields YAML file: {str(e)}") from e

        config_files, _ = resolve_file_paths(config, None)
        cloud_account_id = get_account_id(config_files, account_id)

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

        # Filter for managed jobs if requested
        if managed_only:
            cloud_jobs = [job for job in cloud_jobs if job.identifier is not None]

        cloud_jobs = filter_jobs_by_import_filter(cloud_jobs, filter)

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
        export_jobs_yml(cloud_jobs, include_linked_id, templated_fields)
    except ValueError as e:
        logger.error(f"Error importing jobs: {e}")
        sys.exit(1)


@cli.command()
@option_disable_ssl_verification
@click.argument("config", type=str)
@option_project_ids
@option_environment_ids
@click.option("--dry-run", is_flag=True, help="In dry run mode we don't update dbt Cloud.")
def link(config, project_id, environment_id, dry_run, disable_ssl_verification):
    """
    Link the YML file to dbt Cloud by adding the identifier to the job name.
    All relevant jobs get the part [[...]] added to their name

    The YAML file will need to contain a `linked_id` for each job that needs to be linked.
    """

    config_files, _ = resolve_file_paths(config, None)
    yaml_jobs = load_job_configuration(config_files, None).jobs
    account_id = list(yaml_jobs.values())[0].account_id

    dbt_cloud = DBTCloud(
        account_id=account_id,
        api_key=os.environ.get("DBT_API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
        disable_ssl_verification=disable_ssl_verification,
    )

    # Filter jobs based on project_id and environment_id if provided
    cloud_project_ids = list(project_id) if project_id else None
    cloud_environment_ids = list(environment_id) if environment_id else None

    some_jobs_updated = False
    for current_identifier, job_details in yaml_jobs.items():
        # Skip if job doesn't match project_id filter
        if cloud_project_ids and job_details.project_id not in cloud_project_ids:
            continue
        # Skip if job doesn't match environment_id filter
        if cloud_environment_ids and job_details.environment_id not in cloud_environment_ids:
            continue

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
            logger.success("Updated all jobs!")
        else:
            logger.info("No jobs to link")


@cli.command()
@option_disable_ssl_verification
@click.option(
    "--config",
    type=str,
    help="The path to your YML jobs config file (or pattern for those files).",
)
@click.option("--account-id", type=int, help="The ID of your dbt Cloud account.")
@option_project_ids
@option_environment_ids
@click.option("--dry-run", is_flag=True, help="In dry run mode we don't update dbt Cloud.")
@click.option(
    "--identifier",
    "-i",
    type=str,
    multiple=True,
    help="[Optional] The identifiers we want to unlink. If not provided, all jobs are unlinked.",
)
def unlink(
    config, account_id, project_id, environment_id, dry_run, identifier, disable_ssl_verification
):
    """
    Unlink the YML file to dbt Cloud.
    All relevant jobs get the part [[...]] removed from their name
    """

    defined_jobs = None
    # we get the account id either from a parameter (e.g if the config file doesn't exist) or from the config file
    if account_id:
        cloud_account_id = account_id
    elif config:
        # we get the account id from the config file
        config_files, _ = resolve_file_paths(config, None)
        defined_jobs = load_job_configuration(config_files, None).jobs
        cloud_account_id = list(defined_jobs.values())[0].account_id
    else:
        raise click.BadParameter("Either --config or --account-id must be provided")

    if project_id:
        project_ids = list(project_id)
    else:
        project_ids = None
    if environment_id:
        environment_ids = list(environment_id)
    else:
        environment_ids = None

    dbt_cloud = DBTCloud(
        account_id=cloud_account_id,
        api_key=os.environ.get("DBT_API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
        disable_ssl_verification=disable_ssl_verification,
    )
    cloud_jobs = dbt_cloud.get_jobs(project_ids=project_ids, environment_ids=environment_ids)
    selected_jobs = [job for job in cloud_jobs if job.identifier is not None]
    logger.info("Getting the jobs definition from dbt Cloud")

    # Apply project_id and environment_id filters if provided
    if project_id:
        selected_jobs = [job for job in selected_jobs if job.project_id in project_id]
    if environment_id:
        selected_jobs = [job for job in selected_jobs if job.environment_id in environment_id]
    if identifier:
        selected_jobs = [job for job in selected_jobs if job.identifier in identifier]
    if defined_jobs:
        selected_jobs = [job for job in selected_jobs if job.identifier in defined_jobs]

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
        logger.info("No jobs to unlink")
    elif not dry_run:
        logger.success("Updated all jobs!")


@cli.command()
@option_disable_ssl_verification
@click.option(
    "--config",
    type=str,
    help="The path to your YML jobs config file (or pattern for those files).",
)
@click.option("--account-id", type=int, help="The ID of your dbt Cloud account.")
@option_project_ids
@option_environment_ids
@click.option(
    "--job-id",
    "-j",
    type=int,
    multiple=True,
    help="The ID of the job to deactivate.",
)
def deactivate_jobs(
    config, account_id, project_id, environment_id, job_id, disable_ssl_verification
):
    """
    Deactivate jobs triggers in dbt Cloud (schedule and CI/CI triggers) without remoing the jobs.

    This can be useful when moving jobs from one project to another.
    When the new jobs have been created, this command can be used to deactivate the jobs from the old project.
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

    # First filter by job_id if provided
    selected_cloud_jobs = cloud_jobs
    if job_id:
        selected_cloud_jobs = [job for job in cloud_jobs if job.id in job_id]

    # Then apply project_id and environment_id filters if provided
    if project_id:
        selected_cloud_jobs = [job for job in selected_cloud_jobs if job.project_id in project_id]
    if environment_id:
        selected_cloud_jobs = [
            job for job in selected_cloud_jobs if job.environment_id in environment_id
        ]

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

    logger.success("Deactivated all jobs!")


@cli.command(hidden=True)
def update_json_schema():
    json_schema = generate_config_schema()
    Path("src/dbt_jobs_as_code/schemas/load_job_schema.json").write_text(json_schema)


if __name__ == "__main__":
    cli()
