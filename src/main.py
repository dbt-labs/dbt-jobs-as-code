import os
import yaml

from schemas.job import JobDefinition
from client import DBTCloud

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

    dbt_cloud = DBTCloud(account_id=43791, api_key=os.environ.get('API_KEY'))

    response = dbt_cloud.create_job(pydantic_job_definition)
    print(response)

    response.generate_docs = True
    updated_job = dbt_cloud.update_job(response)

    print(updated_job)