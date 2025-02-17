from beartype.typing import Any, Dict, Optional, Tuple
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


def check_job_mapping_same(
    source_job: JobDefinition, dest_job: JobDefinition
) -> Tuple[bool, Optional[Dict]]:
    """Checks if the source and destination jobs are the same

    Returns:
        Tuple[bool, Optional[Dict]]: A tuple containing:
            - bool: True if jobs are identical, False otherwise
            - Optional[Dict]: None if jobs are identical, otherwise a dict containing the differences
    """
    source_job_dict = _job_to_dict(source_job)
    dest_job_dict = _job_to_dict(dest_job)

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
) -> Tuple[bool, Optional[int], Optional[Dict]]:
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
