import glob
import json
import os
import string
from collections import Counter
from typing import Dict, Optional

from beartype import BeartypeConf, BeartypeStrategy, beartype
from beartype.typing import Callable, List
from loguru import logger
from pydantic import BaseModel
from rich.console import Console
from rich.table import Table

from dbt_jobs_as_code.client import DBTCloud, DBTCloudException
from dbt_jobs_as_code.loader.load import LoadingJobsYAMLError, load_job_configuration
from dbt_jobs_as_code.schemas import check_env_var_same, check_job_mapping_same
from dbt_jobs_as_code.schemas.job import JobDefinition

# Dynamically create a new @nobeartype decorator disabling type-checking.
nobeartype = beartype(conf=BeartypeConf(strategy=BeartypeStrategy.O0))


def json_serializer_type(obj):
    if isinstance(obj, type):
        return obj.__name__
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class Change(BaseModel):
    """Describes what a given change is and how to apply it."""

    identifier: str
    type: str
    action: str
    proj_id: int
    env_id: int
    sync_function: Callable
    parameters: dict
    differences: Optional[Dict] = {}

    def __str__(self):
        return f"{self.action.upper()} {string.capwords(self.type)} {self.identifier}"

    def apply(self):
        self.sync_function(**self.parameters)


class ChangeSet(BaseModel):
    """Store the set of changes to be displayed or applied."""

    root: List[Change] = []
    apply_success: bool = True

    def __iter__(self):
        return iter(self.root)

    def append(self, change: Change):
        self.root.append(change)

    def __str__(self):
        list_str = [str(change) for change in self.root]
        return "\n".join(list_str)

    def to_table(self) -> Table:
        """Return a table representation of the changeset."""

        table = Table(title="Changes detected")

        table.add_column("Action", style="cyan", no_wrap=True)
        table.add_column("Type", style="magenta")
        table.add_column("ID", style="green")
        table.add_column("Proj ID", style="yellow")
        table.add_column("Env ID", style="red")

        for change in self.root:
            table.add_row(
                change.action.upper(),
                string.capwords(change.type),
                change.identifier,
                str(change.proj_id),
                str(change.env_id),
            )

        return table

    def to_json(self) -> dict:
        """Return a structured JSON representation of the changeset."""
        job_changes = []
        env_var_changes = []

        for change in self.root:
            # Create the base change dictionary for overall changes
            overall_change_dict = {
                "action": change.action.upper(),
                "type": string.capwords(change.type),
                "identifier": change.identifier,
                "project_id": change.proj_id,
                "environment_id": change.env_id,
                "differences": change.differences,
            }

            if change.type == "job":
                job_changes.append(overall_change_dict)
            elif change.type == "env var overwrite":
                env_var_changes.append(overall_change_dict)

        return {
            "job_changes": job_changes,
            "env_var_overwrite_changes": env_var_changes,
        }

    def __len__(self):
        return len(self.root)

    def apply(self):
        for change in self.root:
            try:
                change.apply()
            except DBTCloudException:
                self.apply_success = False


# Don't bear type this function as we do some odd things in tests
@nobeartype
def filter_config(
    defined_jobs: dict[str, JobDefinition], project_ids: List[int], environment_ids: List[int]
) -> dict[str, JobDefinition]:
    """Filters the config based on the inputs provided for project ids and environment ids."""
    removed_job_ids: set[str] = set()
    if len(environment_ids) != 0:
        for job_id, job in defined_jobs.items():
            if job.environment_id not in environment_ids:
                removed_job_ids.add(job_id)
                logger.warning(
                    f"For Job# {job.identifier}, environment_id(s) provided as arguments does not match the ID's in Jobs YAML file!!!"
                )
    if len(project_ids) != 0:
        for job_id, job in defined_jobs.items():
            if job.project_id not in project_ids:
                removed_job_ids.add(job_id)
                logger.warning(
                    f"For Job# {job.identifier}, project_id(s) provided as arguments does not match the ID's in Jobs YAML file!!!"
                )
    return {job_id: job for job_id, job in defined_jobs.items() if job_id not in removed_job_ids}


def _check_no_duplicate_job_identifier(remote_jobs: List[JobDefinition]):
    """Check if there are duplicate job identifiers in dbt Cloud.

    If so, raise some error logs"""
    count_identifiers = Counter(
        [job.identifier for job in remote_jobs if job.identifier is not None]
    )
    multiple_counts = {item: count for item, count in count_identifiers.items() if count > 1}

    jobs_affected = [job for job in remote_jobs if job.identifier in multiple_counts]
    for job in jobs_affected:
        logger.error(
            f"The job {job.id} has a duplicate identifier '{job.identifier}' in dbt Cloud: {job.to_url(account_url=os.environ.get('DBT_BASE_URL', 'https://cloud.getdbt.com'))}"
        )


def _check_single_account_id(defined_jobs: List[JobDefinition]):
    """Check if there are duplicate job identifiers in dbt Cloud.

    If so, raise some error logs"""
    count_account_ids = Counter([job.account_id for job in defined_jobs])

    if len(count_account_ids) > 1:
        logger.error(
            f"There are different account_id in the jobs YAML file: {count_account_ids.keys()}"
        )


def build_change_set(
    config: str,
    yml_vars: str,
    disable_ssl_verification: bool,
    project_ids: List[int],
    environment_ids: List[int],
    limit_projects_envs_to_yml: bool = False,
    output_json: bool = False,
):
    """Compares the config of YML files versus dbt Cloud.
    Depending on the value of no_update, it will either update the dbt Cloud config or not.

    CONFIG is the path to your jobs.yml config file.
    """

    # If the config is a directory, we automatically search for all the `*.yml` files in this directory
    if os.path.isdir(config):
        config = os.path.join(config, "*.yml")
    # Get list of files matching the glob pattern
    config_files = glob.glob(config)
    if not config_files:
        logger.error(f"No files found matching pattern: {config}")
        return ChangeSet()

    yml_vars_files = glob.glob(yml_vars) if yml_vars else None

    try:
        configuration = load_job_configuration(config_files, yml_vars_files)
    except (LoadingJobsYAMLError, KeyError) as e:
        logger.error(f"Error loading jobs YAML file ({type(e).__name__}): {e}")
        exit(1)

    if limit_projects_envs_to_yml:
        # if limit_projects_envs_to_yml is True, we keep all the YML jobs
        defined_jobs = configuration.jobs
        # and only the remote jobs with project_id and environment_id existing in the job YML file are considered
        project_ids = list({job.project_id for job in defined_jobs.values()})
        environment_ids = list({job.environment_id for job in defined_jobs.values()})

    else:
        # If a project_id or environment_id is passed in as a parameter (one or multiple), check if these match the ID's in Jobs YAML file, otherwise add a warning and continue the process
        unfiltered_defined_jobs = configuration.jobs
        defined_jobs = filter_config(unfiltered_defined_jobs, project_ids, environment_ids)

    if len(defined_jobs) == 0:
        logger.warning(
            "No jobs found in the Jobs YAML file after filtering based on the project_id and environment_id provided as arguments!!!"
        )
        return ChangeSet()

    _check_single_account_id(list(defined_jobs.values()))

    dbt_cloud = DBTCloud(
        account_id=list(defined_jobs.values())[0].account_id,
        api_key=os.environ.get("DBT_API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
        disable_ssl_verification=disable_ssl_verification,
    )

    cloud_jobs = dbt_cloud.get_jobs(project_ids=project_ids, environment_ids=environment_ids)
    _check_no_duplicate_job_identifier(cloud_jobs)
    tracked_jobs = {job.identifier: job for job in cloud_jobs if job.identifier is not None}

    dbt_cloud_change_set = ChangeSet()

    # Use sets to find jobs for different operations
    shared_jobs = set(defined_jobs.keys()).intersection(set(tracked_jobs.keys()))
    created_jobs = set(defined_jobs.keys()) - set(tracked_jobs.keys())
    deleted_jobs = set(tracked_jobs.keys()) - set(defined_jobs.keys())

    # Update changed jobs
    if not output_json:
        logger.info("Detected {count} existing jobs.", count=len(shared_jobs))
    for identifier in shared_jobs:
        if not output_json:
            logger.info("Checking for differences in {identifier}", identifier=identifier)
        is_same, diff_data = check_job_mapping_same(
            source_job=defined_jobs[identifier], dest_job=tracked_jobs[identifier]
        )
        if not is_same:
            dbt_cloud_change = Change(
                identifier=identifier,
                type="job",
                action="update",
                proj_id=defined_jobs[identifier].project_id,
                env_id=defined_jobs[identifier].environment_id,
                sync_function=dbt_cloud.update_job,
                parameters={"job": defined_jobs[identifier]},
                differences=diff_data.get("differences", {}) if diff_data else {},
            )
            dbt_cloud_change_set.append(dbt_cloud_change)
            defined_jobs[identifier].id = tracked_jobs[identifier].id
            if not output_json:
                console = Console()
                console.print(
                    f"❌ Job {identifier} is different - Diff:\n{json.dumps(diff_data, indent=2, default=json_serializer_type)}"
                )
        elif not output_json:
            logger.success(f"✅ Job {identifier} is identical")

    # Create new jobs
    if not output_json:
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
    if not output_json:
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
    if not output_json:
        logger.debug(f"Mapping of job identifier to id: {mapping_job_identifier_job_id}")

    # Replicate the env vars from the YML to dbt Cloud
    for job in defined_jobs.values():
        if job.identifier in mapping_job_identifier_job_id:  # the job already exists
            job_id = mapping_job_identifier_job_id[job.identifier]
            all_env_vars_for_job = dbt_cloud.get_env_vars(project_id=job.project_id, job_id=job_id)
            for env_var_yml in job.custom_environment_variables:
                env_var_yml.job_definition_id = job_id
                same_env_var, env_var_id, diff_data = check_env_var_same(
                    source_env_var=env_var_yml, dest_env_vars=all_env_vars_for_job
                )
                if not same_env_var and diff_data:
                    action = (
                        "CREATE"
                        if diff_data.get("old_value") is None
                        else "DELETE"
                        if diff_data.get("new_value") is None
                        else "UPDATE"
                    )
                    dbt_cloud_change = Change(
                        identifier=f"{job.identifier}:{env_var_yml.name}",
                        type="env var overwrite",
                        action=action,
                        proj_id=job.project_id,
                        env_id=job.environment_id,
                        sync_function=dbt_cloud.update_env_var,
                        parameters={
                            "project_id": job.project_id,
                            "job_id": job_id,
                            "custom_env_var": env_var_yml,
                            "env_var_id": env_var_id,
                        },
                        differences=diff_data,
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
                    if not output_json:
                        logger.info(f"{env_var} not in the YML file but in the dbt Cloud job")
                    dbt_cloud_change = Change(
                        identifier=f"{job.identifier}:{env_var}",
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

    return dbt_cloud_change_set
