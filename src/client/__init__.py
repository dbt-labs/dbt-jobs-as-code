from typing import Dict, List, Optional
from urllib3.exceptions import InsecureRequestWarning

import requests
from loguru import logger

from src.schemas.custom_environment_variable import (
    CustomEnvironmentVariable,
    CustomEnvironmentVariablePayload,
)
from src.schemas.job import JobDefinition

VERSION = "0.4.0"


class DBTCloud:
    """A minimalistic API client for fetching dbt Cloud data."""

    def __init__(
        self,
        account_id: int,
        api_key: str,
        base_url: str = "https://cloud.getdbt.com",
        disable_ssl_verification: bool = False,
    ) -> None:
        self.account_id = account_id
        self._api_key = api_key
        self._environment_variable_cache: Dict[
            int, Dict[str, CustomEnvironmentVariablePayload]
        ] = {}

        self.base_url = base_url
        self._headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "User-Agent": f"dbt-jobs-as-code/v{VERSION}",
        }
        self._verify = not disable_ssl_verification
        if not self._verify:
            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
            logger.warning(
                "SSL verification is disabled. This is not recommended unless you absolutely need this config."
            )

    def _clear_env_var_cache(self, job_definition_id: int) -> None:
        """Clear out any cached environment variables for a given job."""
        if job_definition_id in self._environment_variable_cache:
            del self._environment_variable_cache[job_definition_id]

    def _check_for_creds(self):
        """Confirm the presence of credentials"""
        if not self._api_key:
            raise Exception("An API key is required to get dbt Cloud jobs.")

        if not self.account_id:
            raise Exception("An account_id is required to get dbt Cloud jobs.")

    def build_mapping_job_identifier_job_id(self, cloud_jobs: List[JobDefinition] = None):


        if cloud_jobs is None:
            # TODO, we should filter things here at least if we call it often
            cloud_jobs = self.get_jobs()

        mapping_job_identifier_job_id = {}
        for job in cloud_jobs:
            if job.identifier is not None:
                mapping_job_identifier_job_id[job.identifier] = job.id

        return mapping_job_identifier_job_id

    def update_job(self, job: JobDefinition) -> JobDefinition:
        """Update an existing dbt Cloud job using a new JobDefinition"""

        logger.debug("Updating {job_name}. {job}", job_name=job.name, job=job)

        response = requests.post(  # Yes, it's actually a POST. Ew.
            url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/{job.id}/",
            headers=self._headers,
            data=job.to_payload(),
            verify=self._verify,
        )

        if response.status_code >= 400:
            logger.error(response.json())

        logger.success("Job updated successfully.")

        return JobDefinition(**(response.json()["data"]), identifier=job.identifier)

    def create_job(self, job: JobDefinition) -> JobDefinition:
        """Create a dbt Cloud Job using a JobDefinition"""

        logger.debug("Creating {job_name}. {job}", job_name=job.name, job=job)

        response = requests.post(
            url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/",
            headers=self._headers,
            data=job.to_payload(),
            verify=self._verify,
        )

        if response.status_code >= 400:
            logger.error(response.json())
            return None

        logger.success("Job created successfully.")

        return JobDefinition(**(response.json()["data"]), identifier=job.identifier)

    def delete_job(self, job: JobDefinition) -> None:
        """Delete a dbt Cloud job."""

        logger.debug("Deleting {job_name}. {job}", job_name=job.name, job=job)

        response = requests.delete(
            url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/{job.id}/",
            headers=self._headers,
            verify=self._verify,
        )

        if response.status_code >= 400:
            logger.error(response.json())

        logger.success("Job deleted successfully.")

    def get_jobs(self, project_id=None , environment_id=None) -> List[JobDefinition]:
        """Return a list of Jobs for all the dbt Cloud jobs in an environment."""

        self._check_for_creds()

        offset = 0
        list_env_ids = []
        multiple_envs_passed = False
        jobs = []

        if project_id is None:
            project_id = []
        if environment_id is None:
            environment_id = []

        if len(environment_id) > 1:
            multiple_envs_passed = True
            list_env_ids = environment_id

        # Handle the scenario, where multiple environment ID's are passed through CLI. Invoke the API once for each environment and append the jobs together
        if multiple_envs_passed:

            for env_id in list_env_ids:

                while True:
                    parameters = {"offset": offset}
                    parameters["environment_id"]=env_id

                    if len(project_id) == 1:
                        parameters["project_id"]=project_id[0]
                    elif len(project_id) > 1:
                        project_id_str = [str(i) for i in project_id]
                        parameters["project_id__in"]=f"[{','.join(project_id_str)}]"

                    response = requests.get(
                        url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/",
                        params=parameters,
                        headers=self._headers,
                        verify=self._verify,
                    )

                    job_data = response.json()

                    if response.status_code >= 400:
                        logger.error(job_data)

                    jobs.extend(job_data["data"])

                    if (
                        job_data["extra"]["filters"]["limit"] + job_data["extra"]["filters"]["offset"]
                        >= job_data["extra"]["pagination"]["total_count"]
                    ):
                        break

                    offset += job_data["extra"]["filters"]["limit"]

        else: 
            # In this case, there are no multiple environments ID's.. Invoke the API once
            while True:
                parameters = {"offset": offset}
                if len(project_id) == 1:
                    parameters["project_id"]=project_id[0]
                elif len(project_id) > 1:
                    project_id_str = [str(i) for i in project_id]
                    parameters["project_id__in"]=f"[{','.join(project_id_str)}]"

                if len(environment_id) == 1:
                    parameters["environment_id"]=environment_id[0]

                logger.debug(f"Request parameters {parameters}")
                response = requests.get(
                    url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/",
                    params=parameters,
                    headers=self._headers,
                    verify=self._verify,
                )

                job_data = response.json()
                if response.status_code >= 400:
                    logger.error(job_data)
                    return []

                jobs.extend(job_data["data"])

                if (
                    job_data["extra"]["filters"]["limit"] + job_data["extra"]["filters"]["offset"]
                    >= job_data["extra"]["pagination"]["total_count"]
                ):
                    break

                offset += job_data["extra"]["filters"]["limit"]

        return [JobDefinition(**job) for job in jobs]

    def get_job(self, job_id: int) -> JobDefinition:
        """Generate a Job based on a dbt Cloud job."""

        self._check_for_creds()

        response = requests.get(
            url=(f"{self.base_url}/api/v2/accounts/" f"{self.account_id}/jobs/{job_id}"),
            headers=self._headers,
            verify=self._verify,
        )
        return JobDefinition(**response.json()["data"])

    def get_env_vars(
        self, project_id: int, job_id: int
    ) -> Dict[str, CustomEnvironmentVariablePayload]:
        """Get the existing env vars job overwrite in dbt Cloud."""

        if job_id in self._environment_variable_cache:
            return self._environment_variable_cache[job_id]

        self._check_for_creds()

        response = requests.get(
            url=(
                f"{self.base_url}/api/v3/accounts/{self.account_id}/projects/{project_id}/environment-variables/job/?job_definition_id={job_id}"
            ),
            headers=self._headers,
            verify=self._verify,
        )

        variables = {
            name: CustomEnvironmentVariablePayload(
                id=variable_data.get("job", {}).get("id"),
                name=name,
                value=variable_data.get("job", {}).get("value"),
                job_definition_id=job_id,
                project_id=project_id,
                account_id=self.account_id,
            )
            for name, variable_data in response.json()["data"].items()
        }
        self._environment_variable_cache[job_id] = variables

        return variables

    def create_env_var(
        self, env_var: CustomEnvironmentVariablePayload
    ) -> CustomEnvironmentVariablePayload:
        """Create a new Custom Environment Variable in dbt Cloud."""

        response = requests.post(
            f"{self.base_url}/api/v3/accounts/{self.account_id}/projects/{env_var.project_id}/environment-variables/",
            headers=self._headers,
            data=env_var.json(),
            verify=self._verify,
        )
        logger.debug(response.json())

        if response.status_code >= 400:
            logger.error(response.json())

        # If the new environment variables has a job_definition_id, then clear the EnvVar cache.
        self._clear_env_var_cache(job_definition_id=env_var.job_definition_id)

        return response.json()["data"]

    def update_env_var(
        self,
        custom_env_var: CustomEnvironmentVariable,
        project_id: int,
        job_id: int,
        env_var_id: int,
        yml_job_identifier: str = None,
    ) -> Optional[CustomEnvironmentVariablePayload]:
        """Update env vars job overwrite in dbt Cloud."""

        self._check_for_creds()

        # handle the case where the job was not created when we queued the function call
        if yml_job_identifier and not job_id:
            mapping_job_identifier_job_id = self.build_mapping_job_identifier_job_id()
            job_id = mapping_job_identifier_job_id[yml_job_identifier]
            custom_env_var.job_definition_id = job_id

        # the endpoint is different for updating an overwrite vs creating one
        if env_var_id:
            url = f"{self.base_url}/api/v3/accounts/{self.account_id}/projects/{project_id}/environment-variables/{env_var_id}/"
        else:
            url = f"{self.base_url}/api/v3/accounts/{self.account_id}/projects/{project_id}/environment-variables/"

        payload = CustomEnvironmentVariablePayload(
            account_id=self.account_id,
            project_id=project_id,
            id=env_var_id,
            **custom_env_var.dict(),
        )

        response = requests.post(
            url=url, headers=self._headers, data=payload.json(), verify=self._verify
        )

        if response.status_code >= 400:
            logger.error(response.json())

        self._clear_env_var_cache(job_definition_id=payload.job_definition_id)

        logger.success(f"Updated the env_var {custom_env_var.name} for job {job_id}")
        return CustomEnvironmentVariablePayload(**(response.json()["data"]))

    def delete_env_var(self, project_id: int, env_var_id: int) -> None:
        """Delete env_var job overwrite in dbt Cloud."""

        logger.debug(f"Deleting env var id {env_var_id}")

        response = requests.delete(
            url=f"{self.base_url}/api/v3/accounts/{self.account_id}/projects/{project_id}/environment-variables/{env_var_id}/",
            headers=self._headers,
            verify=self._verify,
        )

        if response.status_code >= 400:
            logger.error(response.json())

        logger.success("Env Var Job Overwrite deleted successfully.")

    def get_environments(self) -> Dict:
        """Return a list of Environments for all the dbt Cloud jobs in an account"""

        self._check_for_creds()

        response = requests.get(
            url=(f"{self.base_url}/api/v3/accounts/" f"{self.account_id}/environments/"),
            headers=self._headers,
            verify=self._verify,
        )

        if response.status_code >= 400:
            logger.error(response.json())
            logger.error(f"Does the Account ID {self.account_id} exist?")

        return response.json()["data"]
