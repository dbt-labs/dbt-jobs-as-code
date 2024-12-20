import json

from beartype.typing import Any, Optional, Tuple
from deepdiff import DeepDiff
from loguru import logger

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


def check_job_mapping_same(source_job: JobDefinition, dest_job: JobDefinition) -> bool:
    """Checks if the source and destination jobs are the same"""

    source_job_dict = _job_to_dict(source_job)
    dest_job_dict = _job_to_dict(dest_job)

    diffs = _get_mismatched_dict_entries(dest_job_dict, source_job_dict)

    if len(diffs) == 0:
        logger.success(f"✅ Job {source_job.identifier} is identical")
        return True
    else:
        # we can't json.dump types normally, so we provide a custom logic to just return the type name
        def type_serializer(obj):
            if isinstance(obj, type):
                return obj.__name__
            raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

        logger.info(
            f"❌ Job {source_job.identifier} is different - Diff:\n{json.dumps(diffs, indent=2, default=type_serializer)}"
        )
        return False


def check_env_var_same(
    source_env_var: CustomEnvironmentVariable,
    dest_env_vars: dict[str, CustomEnvironmentVariablePayload],
) -> Tuple[bool, Optional[int]]:
    """Checks if the source env vars is the same in the destination env vars"""

    if source_env_var.name not in dest_env_vars:
        raise Exception(
            f"Custom environment variable {source_env_var.name} not found in dbt Cloud, "
            f"you need to create it first."
        )

    env_var_id = dest_env_vars[source_env_var.name].id

    if dest_env_vars[source_env_var.name].value == source_env_var.value:
        logger.debug(
            f"The env var {source_env_var.name} is already up to date for the job {source_env_var.job_definition_id}."
        )
        return (True, env_var_id)
    else:
        logger.info(f"❌ The env var overwrite for {source_env_var.name} is different")
        return (False, env_var_id)
