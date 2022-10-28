from typing import Any

from deepdiff import DeepDiff
from loguru import logger

from schemas.job import JobDefinition


def _get_mismatched_dict_entries(
    dict_source: dict[str, Any], dict_dest: dict[str, Any]
) -> dict[str, Any]:
    """Returns a dict with the mismatched entries between two dicts"""

    return DeepDiff(dict_source, dict_dest, ignore_order=True)


def _job_to_dict(job: JobDefinition):
    dict_vals = job.dict(
        exclude={
            "id",  # we want to exclude id because our YAML file will not have it
            "custom_environment_variables",  # TODO: Add this back in. Requires extra API calls.
        }
    )
    return dict_vals


def check_job_mapping_same(source_job: JobDefinition, dest_job: JobDefinition) -> bool:
    """ " Checks if the source and destination jobs are the same"""

    source_job_dict = _job_to_dict(source_job)
    dest_job_dict = _job_to_dict(dest_job)

    diffs = _get_mismatched_dict_entries(source_job_dict, dest_job_dict)
    # breakpoint()

    if len(diffs) == 0:
        logger.success(f"✅ Jobs identical")
        return True
    else:
        logger.warning(f"❌ Jobs are different - Diff: {diffs}")
        return False
