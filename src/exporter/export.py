from ruamel.yaml import YAML
import sys

from src.schemas.job import JobDefinition


def export_jobs_yml(jobs: list[JobDefinition]):
    """Export a list of job definitions to YML"""

    export_yml = {"jobs": {}}
    for id, cloud_job in enumerate(jobs):
        export_yml["jobs"][f"import_{id + 1}"] = cloud_job.to_load_format()

    yaml = YAML()
    yaml.width = 300
    yaml.dump(export_yml, sys.stdout)
