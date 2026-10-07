import json
import os
from collections import Counter
from dataclasses import dataclass

from loguru import logger
from rich.console import Console
from rich.table import Table

from dbt_jobs_as_code.client import DBTCloud
from dbt_jobs_as_code.cloud_yaml_mapping.change_set import (
    Change,
    ChangeSet,
    json_serializer_type,
)
from dbt_jobs_as_code.loader.load import load_job_configuration, resolve_file_paths
from dbt_jobs_as_code.schemas import check_job_mapping_same
from dbt_jobs_as_code.schemas.job import JobDefinition


class UpdateJobsError(Exception):
    """The YAML can't be applied safely. Nothing has been updated in dbt Cloud."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("\n".join(errors))


@dataclass
class UpdateJobsOptions:
    """Inputs for build_update_change_set."""

    config: str
    yml_vars: str | None
    disable_ssl_verification: bool
    project_ids: list[int]
    environment_ids: list[int]
    use_desc_for_id: bool = False


def _select_yaml_jobs(
    jobs: dict[str, JobDefinition], project_ids: list[int], environment_ids: list[int]
) -> dict[str, JobDefinition]:
    return {
        key: job
        for key, job in jobs.items()
        if (not project_ids or job.project_id in project_ids)
        and (not environment_ids or job.environment_id in environment_ids)
    }


def _validate_yaml_jobs(jobs: dict[str, JobDefinition]) -> list[str]:
    """Checks that only need the YAML: every job must say which dbt Cloud job it is."""
    errors = []
    for key, job in jobs.items():
        if job.linked_id is None:
            errors.append(
                f"Job '{key}' has no `linked_id`. update-jobs matches YAML jobs to dbt Cloud "
                "jobs by `linked_id` (use `import-jobs --include-linked-id` to export it)."
            )

    count_linked_ids = Counter(job.linked_id for job in jobs.values() if job.linked_id)
    for linked_id, count in count_linked_ids.items():
        if count > 1:
            keys = [key for key, job in jobs.items() if job.linked_id == linked_id]
            errors.append(f"`linked_id` {linked_id} is used by several jobs: {', '.join(keys)}")

    account_ids = {job.account_id for job in jobs.values()}
    if len(account_ids) > 1:
        errors.append(f"The YAML jobs belong to different accounts: {sorted(account_ids)}")
    return errors


def _validate_against_cloud(
    key: str, yaml_job: JobDefinition, cloud_job: JobDefinition | None, use_desc_for_id: bool
) -> str | None:
    """Checks that need the live job. Returns an error message, or None if all good."""
    if cloud_job is None:
        return (
            f"Job '{key}': `linked_id` {yaml_job.linked_id} doesn't exist in dbt Cloud "
            f"(project {yaml_job.project_id}, environment {yaml_job.environment_id})."
        )
    # Jobs managed with `sync` are matched by their identifier: letting update-jobs touch
    # one under a different key would leave two files claiming the same job.
    if cloud_job.identifier is not None and cloud_job.identifier != key:
        return (
            f"Job '{key}': `linked_id` {yaml_job.linked_id} is managed by `sync` under the "
            f"identifier '{cloud_job.identifier}'. Use that identifier as the key in the YAML, "
            "or use `sync` for this job."
        )
    # A job managed with --use-desc-for-id looks unmanaged when the flag is forgotten, and a
    # YAML exported with the flag has a clean description: updating would strip the tag
    # from the description, which silently unlinks the job.
    if not use_desc_for_id and cloud_job.description:
        tag = JobDefinition._extract_identifier_from_description(cloud_job.description)
        if tag.identifier:
            return (
                f"Job '{key}': the description of job {yaml_job.linked_id} contains "
                f"[[{tag.raw_identifier}]]. If your jobs are managed with the identifier in the "
                "description, pass `--use-desc-for-id`, otherwise this update would unlink the job."
            )
    return None


def build_update_change_set(options: UpdateJobsOptions) -> ChangeSet:
    """Compare the jobs in the YAML with the dbt Cloud jobs they point to (`linked_id`)
    and return a ChangeSet containing one update per job that differs.

    Only existing jobs are updated, they are never created, deleted, or (un)linked: the
    [[identifier]] of a job is left as it is in dbt Cloud, whatever the key in the YAML is.

    Raises UpdateJobsError, before anything is changed, if some YAML jobs can't be matched.
    """
    config_files, vars_files = resolve_file_paths(options.config, options.yml_vars)
    configuration = load_job_configuration(config_files, vars_files or None)
    yaml_jobs = _select_yaml_jobs(configuration.jobs, options.project_ids, options.environment_ids)
    if len(yaml_jobs) < len(configuration.jobs):
        logger.info(
            "Skipping {count} job(s) of the YAML outside of the project/environment filters",
            count=len(configuration.jobs) - len(yaml_jobs),
        )

    if not yaml_jobs:
        logger.warning("No jobs to update in the YAML (after the project/environment filters)")
        return ChangeSet()

    errors = _validate_yaml_jobs(yaml_jobs)
    if errors:
        raise UpdateJobsError(errors)

    account_id = next(iter(yaml_jobs.values())).account_id
    dbt_cloud = DBTCloud(
        account_id=account_id,
        api_key=os.environ.get("DBT_API_KEY"),
        base_url=os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com"),
        disable_ssl_verification=options.disable_ssl_verification,
        use_desc_for_id=options.use_desc_for_id,
    )

    cloud_jobs = {
        job.id: job
        for job in dbt_cloud.get_jobs(
            project_ids=sorted({job.project_id for job in yaml_jobs.values()}),
            environment_ids=sorted({job.environment_id for job in yaml_jobs.values()}),
        )
    }

    errors = []
    pairs: list[tuple[str, JobDefinition, JobDefinition]] = []
    for key, yaml_job in yaml_jobs.items():
        assert yaml_job.linked_id is not None  # checked in _validate_yaml_jobs
        cloud_job = cloud_jobs.get(yaml_job.linked_id)
        error = _validate_against_cloud(key, yaml_job, cloud_job, options.use_desc_for_id)
        if error:
            errors.append(error)
        elif cloud_job is not None:
            pairs.append((key, yaml_job, cloud_job))
    if errors:
        raise UpdateJobsError(errors)

    change_set = ChangeSet()
    for key, yaml_job, cloud_job in pairs:
        # The loader sets the identifier from the YAML key, but that key is only a label here.
        yaml_job.identifier = cloud_job.identifier
        yaml_job.id = cloud_job.id

        is_same, diff_data = check_job_mapping_same(source_job=yaml_job, dest_job=cloud_job)
        if is_same:
            logger.success(f"✅ Job {key} ({cloud_job.id}) is identical")
            continue

        Console().print(
            f"❌ Job {key} ({cloud_job.id}) is different - Diff:\n"
            f"{json.dumps(diff_data, indent=2, default=json_serializer_type)}"
        )
        # Keep the import filter of `[[filter:identifier]]` names, which the loaded jobs don't carry.
        if cloud_job._filter_import and yaml_job.identifier:
            yaml_job.identifier = f"{cloud_job._filter_import}:{yaml_job.identifier}"
        change_set.append(
            Change(
                identifier=key,
                type="job",
                action="update",
                proj_id=yaml_job.project_id,
                env_id=yaml_job.environment_id,
                sync_function=dbt_cloud.update_job,
                parameters={"job": yaml_job},
                differences=diff_data.get("differences", {}) if diff_data else {},
            )
        )

    if any(job.custom_environment_variables for _, job, _ in pairs):
        logger.warning(
            "`custom_environment_variables` are not updated by update-jobs, use `sync` for those."
        )

    return change_set


def _describe(change: Change) -> str:
    job = change.parameters["job"]
    return f"{change.identifier} (job {job.id}: {job.name})"


def changes_table(change_set: ChangeSet) -> Table:
    """The jobs about to be updated, with the dbt Cloud job ID so they can be told apart."""
    table = Table(title="Jobs to update")
    table.add_column("Key", style="cyan", no_wrap=True)
    table.add_column("Job ID", style="green")
    table.add_column("Job name", style="magenta")
    table.add_column("Proj ID", style="yellow")
    table.add_column("Env ID", style="red")
    for change in change_set:
        job = change.parameters["job"]
        table.add_row(
            change.identifier, str(job.id), job.name, str(change.proj_id), str(change.env_id)
        )
    return table


def log_apply_summary(change_set: ChangeSet) -> None:
    """Say which jobs were updated and which were not: the validation happens before
    anything is updated, but dbt Cloud can still reject an individual update."""
    applied = {applied_change["identifier"] for applied_change in change_set.applied_changes}
    updated = [change for change in change_set if change.identifier in applied]
    not_updated = [change for change in change_set if change.identifier not in applied]

    if updated:
        logger.success(
            "Updated {count} job(s): {jobs}",
            count=len(updated),
            jobs=", ".join(_describe(change) for change in updated),
        )
    if not_updated:
        logger.error(
            "{count} job(s) were NOT updated (failed or skipped by --fail-fast): {jobs}",
            count=len(not_updated),
            jobs=", ".join(_describe(change) for change in not_updated),
        )
