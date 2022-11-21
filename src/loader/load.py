from typing import Dict

import yaml
from loguru import logger

from schemas.job import JobDefinition


def load_job_definitions(config_file) -> Dict[str, JobDefinition]:
    """Load a job YAML file into a dictionary of JobDefinitions"""
    config = yaml.safe_load(config_file)

    date_config = [
        job.get("schedule", {}).get("date", None) for job in config["jobs"].values()
    ]
    time_config = [
        job.get("schedule", {}).get("time", None) for job in config["jobs"].values()
    ]

    if any(date_config):
        logger.warning(
            f"❌ There is some date config under 'schedule > date' in your YML. This data is auto generated and should be deleted"
        )
    if any(time_config):
        logger.warning(
            f"❌ There is some time config under 'schedule > time' in your YML. This data is auto generated and should be deleted"
        )

    return {
        identifier: JobDefinition(**job, identifier=identifier)
        for identifier, job in config["jobs"].items()
    }
