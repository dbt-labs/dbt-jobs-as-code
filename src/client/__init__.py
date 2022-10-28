from typing import Dict, List

import requests
from loguru import logger

from schemas.job import JobDefinition


class DBTCloud:
    """A minimalistic API client for fetching dbt Cloud data."""

    def __init__(
        self, account_id: int, api_key: str, base_url: str = "https://cloud.getdbt.com"
    ) -> None:
        self.account_id = account_id
        self._api_key = api_key
        self._manifests: Dict = {}

        self.base_url = base_url
        self._headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

    def _check_for_creds(self):
        """Confirm the presence of credentials"""
        if not self._api_key:
            raise Exception("An API key is required to get dbt Cloud jobs.")

        if not self.account_id:
            raise Exception("An account_id is required to get dbt Cloud jobs.")

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
        logger.warning("Deletion not yet implemented.")
        return

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
