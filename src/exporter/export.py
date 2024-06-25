import sys

from ruamel.yaml import YAML

from src.schemas.job import JobDefinition


def export_jobs_yml(jobs: list[JobDefinition]):
    """Export a list of job definitions to YML"""

    export_yml = {"jobs": {}}
    for id, cloud_job in enumerate(jobs):
        export_yml["jobs"][f"import_{id + 1}"] = cloud_job.to_load_format()

    print(
        "# yaml-language-server: $schema=https://raw.githubusercontent.com/dbt-labs/dbt-jobs-as-code/main/src/schemas/load_job_schema.json"
    )
    print("")

    yaml = YAML()
    yaml.width = 300
    yaml.block_seq_indent = 2
    yaml.dump(export_yml, sys.stdout)
