from typing import Dict, List, Optional

import requests
from loguru import logger

from schemas.custom_environment_variable import (
    CustomEnvironmentVariable,
    CustomEnvironmentVariablePayload,
)
from schemas.job import JobDefinition


class DBTCloud:
    """A minimalistic API client for fetching dbt Cloud data."""

    def __init__(
        self, account_id: int, api_key: str, base_url: str = "https://cloud.getdbt.com"
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
        }

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

    def build_mapping_job_identifier_job_id(self):
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
            url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/{job.id}",
            headers=self._headers,
            data=job.to_payload(),
        )

        if response.status_code >= 400:
            logger.error(response.json())

        logger.success("Updated successfully.")

        return JobDefinition(**(response.json()["data"]), identifier=job.identifier)

    def create_job(self, job: JobDefinition) -> JobDefinition:
        """Create a dbt Cloud Job using a JobDefinition"""

        logger.debug("Creating {job_name}. {job}", job_name=job.name, job=job)

        response = requests.post(
            url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/",
            headers=self._headers,
            data=job.to_payload(),
        )

        if response.status_code >= 400:
            logger.error(response.json())

        logger.success("Created successfully.")

        return JobDefinition(**(response.json()["data"]), identifier=job.identifier)

    def delete_job(self, job: JobDefinition) -> None:
        """Delete a dbt Cloud job."""

        logger.debug("Deleting {job_name}. {job}", job_name=job.name, job=job)

        response = requests.delete(
            url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/{job.id}",
            headers=self._headers,
        )

        if response.status_code >= 400:
            logger.error(response.json())

        logger.warning("Deleted successfully.")

    def get_jobs(self) -> List[JobDefinition]:
        """Return a list of Jobs for all the dbt Cloud jobs in an environment."""

        self._check_for_creds()

        offset = 0
        jobs = []

        while True:
            parameters = {"offset": offset}

            response = requests.get(
                url=f"{self.base_url}/api/v2/accounts/{self.account_id}/jobs/",
                params=parameters,
                headers=self._headers,
            )

            job_data = response.json()

            if response.status_code >= 400:
                logger.error(job_data)

            jobs.extend(job_data["data"])

            if (
                job_data["extra"]["filters"]["limit"]
                + job_data["extra"]["filters"]["offset"]
                >= job_data["extra"]["pagination"]["total_count"]
            ):
                break

            offset += job_data["extra"]["filters"]["limit"]

        return [JobDefinition(**job) for job in jobs]

    def get_job(self, job_id: int) -> Dict:
        """Generate a Job based on a dbt Cloud job."""

        self._check_for_creds()

        response = requests.get(
            url=(
                f"{self.base_url}/api/v2/accounts/" f"{self.account_id}/jobs/{job_id}"
            ),
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )
        return response.json()["data"]

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
        )

        variables = {
            name: CustomEnvironmentVariablePayload(
                id=variable_data.get("job",{}).get("id"),
                name=name,
                value=variable_data.get("job",{}).get("value"),
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
        )
        logger.debug(response.json())

        if response.status_code >= 400:
            logger.error(response.json())

        # If the new environment variables has a job_definition_id, then clear the EnvVar cache.
        self._clear_env_var_cache(job_definition_id=env_var.job_definition_id)

        return response.json()["data"]

    def update_env_var(
        self, custom_env_var: CustomEnvironmentVariable, project_id: int, job_id: int
    ) -> Optional[CustomEnvironmentVariablePayload]:
        """Update env vars job overwrite in dbt Cloud."""

        self._check_for_creds()

        all_env_vars = self.get_env_vars(project_id, job_id)

        if custom_env_var.name not in all_env_vars:
            raise Exception(
                f"Custom environment variable {custom_env_var.name} not found in dbt Cloud, "
                f"you need to create it first."
            )

        env_var_id: Optional[int]

        # TODO: Move this logic out of the client layer, and move it into
        #  at least one layer higher up. We want the dbt Cloud client to be
        #  as naive as possible.
        if custom_env_var.name not in all_env_vars:
            return self.create_env_var(
                CustomEnvironmentVariablePayload(
                    account_id=self.account_id,
                    project_id=project_id,
                    **custom_env_var.dict(),
                )
            )

        if all_env_vars[custom_env_var.name].value == custom_env_var.value:
            logger.debug(
                f"The env var {custom_env_var.name} is already up to date for the job {job_id}."
            )
            return None

        env_var_id: int = all_env_vars[custom_env_var.name].id

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
            url=url,
            headers=self._headers,
            data=payload.json(),
        )

        if response.status_code >= 400:
            logger.error(response.json())

        self._clear_env_var_cache(job_definition_id=payload.job_definition_id)

        logger.info(f"Updated the env_var {custom_env_var.name} for job {job_id}")
        return CustomEnvironmentVariablePayload(**(response.json()["data"]))

    def delete_env_var(self, project_id: int, env_var_id: int) -> None:
        """Delete env_var job overwrite in dbt Cloud."""

        logger.debug(f"Deleting env var id {env_var_id}")

        response = requests.delete(
            url=f"{self.base_url}/api/v3/accounts/{self.account_id}/projects/{project_id}/environment-variables/{env_var_id}/",
            headers=self._headers,
        )

        if response.status_code >= 400:
            logger.error(response.json())

        logger.warning("Deleted successfully.")

    def get_environments(self) -> Dict:
        """Return a list of Environments for all the dbt Cloud jobs in an account"""

        self._check_for_creds()

        response = requests.get(
            url=(
                f"{self.base_url}/api/v3/accounts/" f"{self.account_id}/environments/"
            ),
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )
        return response.json()["data"]
