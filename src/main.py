import os
import yaml

from schemas.job import JobDefinition

if __name__ == '__main__':
    absolute_path = os.path.dirname(__file__)
    example_path = '../supporting-code/jobs.yml'
    configs = open(
        os.path.join(absolute_path, example_path),
        'r'
    )
    pyconfigs = yaml.safe_load(configs)
    pydantic_job_definition = JobDefinition(**pyconfigs['jobs'][0])
    print(pydantic_job_definition)
