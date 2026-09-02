from typing import Any

from deepdiff import DeepDiff

from dbt_jobs_as_code.schemas.custom_environment_variable import (
    CustomEnvironmentVariable,
    CustomEnvironmentVariablePayload,
)
from dbt_jobs_as_code.schemas.job import JobDefinition


def _get_mismatched_dict_entries(
    dict_source: dict[str, Any], dict_dest: dict[str, Any]
) -> dict[str, Any]:
    """Returns a dict with the mismatched entries between two dicts"""

    return DeepDiff(dict_source, dict_dest, ignore_order=True)


def _job_to_dict(job: JobDefinition):
    dict_vals = job.model_dump(
        exclude={
            "id",  # we want to exclude id because our YAML file will not have it
            "custom_environment_variables",  # TODO: Add this back in. Requires extra API calls.
            "linked_id",  # we want to exclude linked_id because dbt Cloud doesn't save it
        }
    )
    return dict_vals


def _is_effectively_self_deferring(job: JobDefinition, own_id: int | None) -> bool:
    """Whether a job defers to itself ("This Job" in the dbt Cloud UI), regardless of
    whether that's expressed via the `self_deferring` flag or (how this was done
    before that flag existed) a literal `deferring_job_definition_id` equal to the
    job's own dbt Cloud id."""
    if job.self_deferring:
        return True
    return own_id is not None and job.deferring_job_definition_id == own_id


def _normalize_self_deferring(job_dict: dict[str, Any]) -> None:
    job_dict["self_deferring"] = True
    job_dict["deferring_job_definition_id"] = None


def check_job_mapping_same(
    source_job: JobDefinition, dest_job: JobDefinition
) -> tuple[bool, dict | None]:
    """Checks if the source and destination jobs are the same

    Returns:
        Tuple[bool, Optional[Dict]]: A tuple containing:
            - bool: True if jobs are identical, False otherwise
            - Optional[Dict]: None if jobs are identical, otherwise a dict containing the differences
    """
    source_job_dict = _job_to_dict(source_job)
    dest_job_dict = _job_to_dict(dest_job)

    # dest_job.id is always known (it comes straight from the dbt Cloud API), even
    # when source_job hasn't been matched to a cloud job yet. Use it to canonicalize
    # "defers to itself" to the same shape on both sides before diffing, so the new
    # self_deferring flag and a legacy hardcoded self-id don't show up as a false diff.
    # Only touch the side that's actually self-deferring: if the other side defers to
    # a genuinely different job, its deferring_job_definition_id must stay visible in
    # the diff, otherwise a switch from self-deferring to a real cross-job target
    # would report only a self_deferring flip and hide the actual target change.
    if _is_effectively_self_deferring(source_job, dest_job.id):
        _normalize_self_deferring(source_job_dict)
    if _is_effectively_self_deferring(dest_job, dest_job.id):
        _normalize_self_deferring(dest_job_dict)

    diffs = _get_mismatched_dict_entries(dest_job_dict, source_job_dict)

    if len(diffs) == 0:
        return True, None
    else:
        return False, {
            "job_id": source_job.identifier,
            "status": "different",
            "differences": diffs,
        }


def check_env_var_same(
    source_env_var: CustomEnvironmentVariable,
    dest_env_vars: dict[str, CustomEnvironmentVariablePayload],
) -> tuple[bool, int | None, dict | None]:
    """Checks if the source env vars is the same in the destination env vars

    Returns:
        Tuple[bool, Optional[int], Optional[Dict]]: A tuple containing:
            - bool: True if env vars are identical, False otherwise
            - Optional[int]: The env var ID if it exists
            - Optional[Dict]: None if env vars are identical, otherwise a dict containing the differences
    """
    if source_env_var.name not in dest_env_vars:
        raise Exception(
            f"Custom environment variable {source_env_var.name} not found in dbt Cloud, "
            f"you need to create it first."
        )

    env_var_id = dest_env_vars[source_env_var.name].id

    if dest_env_vars[source_env_var.name].value == source_env_var.value:
        return True, env_var_id, None
    else:
        return (
            False,
            env_var_id,
            {
                "old_value": dest_env_vars[source_env_var.name].value,
                "new_value": source_env_var.value,
            },
        )
