from typing import Dict, List

import requests

from schemas.job import JobDefinition


class DBTCloud:
    """A minimalistic API client for fetching dbt Cloud data."""

    def __init__(self, account_id: int, api_key: str) -> None:
        self.account_id = account_id
        self._api_key = api_key
        self._manifests: Dict = {}

    def _check_for_creds(self):
        """Confirm the presence of credentials"""
        if not self._api_key:
            raise Exception("An API key is required to get dbt Cloud jobs.")

        if not self.account_id:
            raise Exception("An account_id is required to get dbt Cloud jobs.")

    def create_job(self, job: JobDefinition) -> JobDefinition:
        """Create a dbt Cloud Job using a JobDefinition"""
        payload = job.to_payload()

        response = requests.post(
            url=f"https://cloud.getdbt.com/api/v2/accounts/{self.account_id}/jobs/",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            data=payload
        )

        return JobDefinition(**response.json()['data'])

    def get_jobs(self) -> List[JobDefinition]:
        """Return a list of Jobs for all the dbt Cloud jobs in an environment."""

        self._check_for_creds()

        offset = 0
        jobs = []

        while True:
            parameters = {"offset": offset}

            response = requests.get(
                url=f"https://cloud.getdbt.com/api/v2/accounts/{self.account_id}/jobs/",
                params=parameters,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
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
                f"https://cloud.getdbt.com/api/v2/accounts/"
                f"{self.account_id}/jobs/{job_id}"
            ),
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )
        return response.json()["data"]
