from typing import Dict

import yaml

from schemas.job import JobDefinition


def load_job_definitions(path) -> Dict[str, JobDefinition]:
    """Load a job YAML file into a dictionary of JobDefinitions"""
    with open(path, "r") as config_file:
        config = yaml.safe_load(config_file)

    return {
        identifier: JobDefinition(**job, identifier=identifier)
        for identifier, job in config["jobs"].items()
    }
