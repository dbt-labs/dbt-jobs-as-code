import os

import requests
from beartype.typing import Any, Dict, List, Optional
from importlib_metadata import version
from loguru import logger
from urllib3.exceptions import InsecureRequestWarning

from src.schemas.custom_environment_variable import (
    CustomEnvironmentVariable,
    CustomEnvironmentVariablePayload,
)
from src.schemas.job import JobDefinition, JobMissingFields

if os.getenv("DBT_JOB_ID", "") == "":
    VERSION = f'v{version("dbt-jobs-as-code")}'
else:
    VERSION = "dev"


class DBTCloudException(Exception):
    pass


class DBTCloudParamsException(Exception):
    pass


class DBTCloud:
    """A minimalistic API client for fetching dbt Cloud data."""

    def __init__(
        self,
        account_id: int,
        api_key: Optional[str],
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
            "User-Agent": f"dbt-jobs-as-code/{VERSION}",
        }
        self._verify = not disable_ssl_verification
        if not self._verify:
            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)  # type: ignore
            logger.warning(
                "SSL verification is disabled. This is not recommended unless you absolutely need this config."
            )
        self._session = requests.Session()

    def _clear_env_var_cache(self, job_definition_id: Optional[int]) -> None:
        """Clear out any cached environment variables for a given job."""
        if job_definition_id in self._environment_variable_cache:
            del self._environment_variable_cache[job_definition_id]

    def _check_for_creds(self):
        """Confirm the presence of credentials"""
        if not self._api_key:
            raise DBTCloudParamsException("An API key is required to get dbt Cloud jobs.")

        if not self.account_id:
            raise DBTCloudParamsException("An account_id is required to get dbt Cloud jobs.")

    def build_mapping_job_identifier_job_id(
        self, cloud_jobs: Optional[List[JobDefinition]] = None
    ):
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

        response = self._session.post(  # Yes, it's actually a POST. Ew.
            url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/{job.id}/",
            headers=self._headers,
            data=job.to_payload(),
            verify=self._verify,
        )

        if response.status_code >= 400:
            logger.error(response.json())
            raise DBTCloudException(f"Error updating job {job.name}")
        else:
            logger.success("Job updated successfully.")

        return JobDefinition(**(response.json()["data"]), identifier=job.identifier)

    def create_job(self, job: JobDefinition) -> Optional[JobDefinition]:
        """Create a dbt Cloud Job using a JobDefinition"""

        logger.debug("Creating {job_name}. {job}", job_name=job.name, job=job)

        response = self._session.post(
            url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/",
            headers=self._headers,
            data=job.to_payload(),
            verify=self._verify,
        )

        if response.status_code >= 400:
            logger.error(response.json())
            raise DBTCloudException(f"Error creating job {job.name}")
            return None
        else:
            logger.success("Job created successfully.")

        return JobDefinition(**(response.json()["data"]), identifier=job.identifier)

    def delete_job(self, job: JobDefinition) -> None:
        """Delete a dbt Cloud job."""

        logger.debug("Deleting {job_name}. {job}", job_name=job.name, job=job)

        response = self._session.delete(
            url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/{job.id}/",
            headers=self._headers,
            verify=self._verify,
        )

        if response.status_code >= 400:
            logger.error(response.json())
            raise DBTCloudException(f"Error deleting job {job.name}")
        else:
            logger.success("Job deleted successfully.")

    def get_job(self, job_id: int) -> Optional[JobDefinition]:
        """Generate a Job based on a dbt Cloud job."""

        self._check_for_creds()

        response = self._session.get(
            url=(f"{self.base_url}/api/v2/accounts/" f"{self.account_id}/jobs/{job_id}/"),
            headers=self._headers,
            verify=self._verify,
        )
        if response.status_code > 200:
            logger.error(f"Issue getting the job {job_id}")
            raise DBTCloudException(f"Error getting the job {job_id}")
        return JobDefinition(**response.json()["data"])

    def get_job_missing_fields(self, job_id: int) -> Optional[JobMissingFields]:
        """Generate a Job based on a dbt Cloud job."""

        self._check_for_creds()

        response = self._session.get(
            url=(f"{self.base_url}/api/v2/accounts/" f"{self.account_id}/jobs/{job_id}/"),
            headers=self._headers,
            verify=self._verify,
        )
        if response.status_code > 200:
            logger.error(f"Issue getting the job {job_id}")
            raise DBTCloudException(f"Error getting the job {job_id}")
        return JobMissingFields(**response.json()["data"])

    def get_jobs(
        self,
        project_ids: Optional[List[int]] = None,
        environment_ids: Optional[List[int]] = None,
    ) -> List[JobDefinition]:
        """Return a list of Jobs for all the dbt Cloud jobs in an environment."""

        self._check_for_creds()
        project_ids = project_ids or []
        environment_ids = environment_ids or []

        jobs: List[dict] = []
        if len(environment_ids) > 1:
            for env_id in environment_ids:
                jobs.extend(self._fetch_jobs(project_ids, env_id))
        elif len(environment_ids) == 1:
            jobs = self._fetch_jobs(project_ids, environment_ids[0])
        else:
            jobs = self._fetch_jobs(project_ids, None)

        return [JobDefinition(**job) for job in jobs]

    def _fetch_jobs(self, project_ids: List[int], environment_id: Optional[int]) -> List[dict]:
        offset = 0
        jobs: List[dict] = []

        while True:
            parameters = self._build_parameters(project_ids, environment_id, offset)
            job_data = self._make_request(parameters)

            if not job_data:
                return []

            jobs.extend(job_data["data"])

            if (
                job_data["extra"]["filters"]["limit"] + job_data["extra"]["filters"]["offset"]
                >= job_data["extra"]["pagination"]["total_count"]
            ):
                break

            offset += job_data["extra"]["filters"]["limit"]

        return jobs

    def _build_parameters(
        self, project_ids: List[int], environment_id: Optional[int], offset
    ) -> dict[str, Any]:
        parameters = {"offset": offset}

        if len(project_ids) == 1:
            parameters["project_id"] = project_ids[0]
        elif len(project_ids) > 1:
            project_id_str = [str(i) for i in project_ids]
            parameters["project_id__in"] = f"[{','.join(project_id_str)}]"

        if environment_id is not None:
            parameters["environment_id"] = environment_id

        logger.debug(f"Request parameters {parameters}")
        return parameters

    def _make_request(self, parameters: dict[str, Any]):
        response = self._session.get(
            url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/",
            params=parameters,
            headers=self._headers,
            verify=self._verify,
        )

        if response.status_code >= 400:
            error_data = response.json()
            logger.error(error_data)
            return None

        return response.json()

    def get_env_vars(
        self, project_id: int, job_id: int
    ) -> Dict[str, CustomEnvironmentVariablePayload]:
        """Get the existing env vars job overwrite in dbt Cloud."""

        if job_id in self._environment_variable_cache:
            return self._environment_variable_cache[job_id]

        self._check_for_creds()

        response = self._session.get(
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

        response = self._session.post(
            f"{self.base_url}/api/v3/accounts/{self.account_id}/projects/{env_var.project_id}/environment-variables/",
            headers=self._headers,
            data=env_var.model_dump_json(),
            verify=self._verify,
        )
        logger.debug(response.json())

        if response.status_code >= 400:
            logger.error(response.json())
            raise DBTCloudException(f"Error creating the env var {env_var.name}")

        # If the new environment variables has a job_definition_id, then clear the EnvVar cache.
        self._clear_env_var_cache(job_definition_id=env_var.job_definition_id)

        return response.json()["data"]

    def update_env_var(
        self,
        custom_env_var: CustomEnvironmentVariable,
        project_id: int,
        job_id: Optional[int],
        env_var_id: Optional[int],
        yml_job_identifier: Optional[str] = None,
    ) -> Optional[CustomEnvironmentVariablePayload]:
        """Update env vars job overwrite in dbt Cloud."""

        self._check_for_creds()

        # handle the case where the job was not created when we queued the function call
        if yml_job_identifier and not job_id:
            # TODO  - we shouldn't have to call the API so many times
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
            **custom_env_var.model_dump(),
        )

        response = self._session.post(
            url=url, headers=self._headers, data=payload.model_dump_json(), verify=self._verify
        )

        if response.status_code >= 400:
            logger.error(response.json())
            raise DBTCloudException(f"Error updating the env var {custom_env_var.name}")

        self._clear_env_var_cache(job_definition_id=payload.job_definition_id)

        logger.success(f"Updated the env_var {custom_env_var.name} for job {job_id}")
        return CustomEnvironmentVariablePayload(**(response.json()["data"]))

    def delete_env_var(self, project_id: int, env_var_id: int) -> None:
        """Delete env_var job overwrite in dbt Cloud."""

        logger.debug(f"Deleting env var id {env_var_id}")

        response = self._session.delete(
            url=f"{self.base_url}/api/v3/accounts/{self.account_id}/projects/{project_id}/environment-variables/{env_var_id}/",
            headers=self._headers,
            verify=self._verify,
        )

        if response.status_code >= 400:
            logger.error(response.json())
            raise DBTCloudException(f"Error deleting the env var {env_var_id}")

        logger.success("Env Var Job Overwrite deleted successfully.")

    def _fetch_environment(self, url) -> List[dict]:
        response = self._session.get(
            url=url,
            headers=self._headers,
            verify=self._verify,
        )

        if response.status_code >= 400:
            logger.error(response.json())
            logger.error(f"Does the Account ID {self.account_id} exist?")
            return []

        return response.json()["data"]

    def get_environments(self, project_ids: List[int]) -> List[dict]:
        """Return a list of Environments for all the dbt Cloud jobs in an account"""

        self._check_for_creds()

        all_envs = []
        for project_id in project_ids:
            url = (
                f"{self.base_url}/api/v3/accounts/"
                f"{self.account_id}/environments/?project_id={project_id}"
            )
            all_envs.extend(self._fetch_environment(url))
        return all_envs
