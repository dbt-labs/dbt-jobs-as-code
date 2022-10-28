import os

from client import DBTCloud
from loader.load import load_job_definitions

if __name__ == '__main__':
    absolute_path = os.path.dirname(__file__)
    example_path = '../supporting-code/jobs.yml'
    path = os.path.join(absolute_path, example_path)

    defined_jobs = load_job_definitions(path)

    print(defined_jobs)

    dbt_cloud = DBTCloud(
        account_id=43791,
        api_key=os.environ.get('API_KEY')
    )
    cloud_jobs = dbt_cloud.get_jobs()
    tracked_jobs = {job.identifier: job for job in cloud_jobs if job.identifier is not None }

    print(tracked_jobs)

    #
    #
    # response = dbt_cloud.create_job(pydantic_job_definition)
    # print(response)
    #
    # response.generate_docs = True
    # updated_job = dbt_cloud.update_job(response)
    #
    # print(updated_job)