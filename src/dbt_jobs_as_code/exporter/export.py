import sys
import re

from ruamel.yaml import YAML
from io import StringIO
from dbt_jobs_as_code.schemas.job import JobDefinition


def export_jobs_yml(jobs: list[JobDefinition], include_linked_id: bool = False, job_identifier: str = None, jinja_variables: str = None):
    """Export a list of job definitions to YML"""

    if job_identifier is None:
        job_identifier = "import"

    export_yml = {"jobs": {}}
    for id, cloud_job in enumerate(jobs):
        yaml_key = cloud_job.identifier if cloud_job.identifier else f"{job_identifier}_{id + 1}"
        export_yml["jobs"][yaml_key] = cloud_job.to_load_format(include_linked_id)

    # Convert the dictionary to a YAML string
    yaml = YAML()
    yaml.width = 300
    yaml.block_seq_indent = 2
    yaml_stream = StringIO()
    yaml.dump(export_yml, yaml_stream)
    yaml_content = yaml_stream.getvalue()

    # Replace specified variables with their Jinja placeholders
    if jinja_variables is not None:
        jinja_vars_list = jinja_variables.split(',')
        for var in jinja_vars_list:
            var = var.strip()  # Remove any leading/trailing whitespace
            placeholder = f"{{{{ {var} }}}}"
            yaml_content = re.sub(rf'\b{var}\b:.*', f'{var}: {placeholder}', yaml_content)

    print(
        "# yaml-language-server: $schema=https://raw.githubusercontent.com/dbt-labs/dbt-jobs-as-code/main/src/dbt_jobs_as_code/schemas/load_job_schema.json"
    )
    print("")

    print(yaml_content)
