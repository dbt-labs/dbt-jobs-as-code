
from dataclasses import dataclass
from pydantic_models import *
import logging
from deepdiff import DeepDiff
import requests


def _get_mismatched_dict_entries(
    dict_source: dict[str, Any], dict_dest: dict[str, Any]
) -> dict[str, Any]:
    """Returns a dict with the mismatched entries between two dicts"""

    return DeepDiff(dict_source, dict_dest, ignore_order=True)


def _job_to_dict(job: DbtJob):
    dict_vals = job.dict(
        exclude={
            "id", # we want to exclude id because our YAML file will not have it
            "created_at",
            "updated_at",
            "cron_humanized",
            "next_run",
            "next_run_humanized",
            "run_failure_count",
            "is_deferrable",
        }
    )
    return dict_vals


def check_job_mapping_same(source_job: DbtJob, dest_job: DbtJob) -> bool:
    """" Checks if the source and destination jobs are the same"""

    source_job_dict = _job_to_dict(source_job)
    dest_job_dict = _job_to_dict(dest_job)

    diffs = _get_mismatched_dict_entries(source_job_dict, dest_job_dict)
    # breakpoint()

    if len(diffs) == 0:
        logging.info(f"✅ Jobs identical")
        return True
    else:
        logging.warning(
            f"❌ Jobs are different - Diff: {diffs}"
        )
        return False


@dataclass
class APICalls:
    url: str
    account_id: int
    token: str

    def get_dbt_cloud_api(
        self, endpoint: str, version: str = "v2"
    ) -> dict:

        url = self.url
        account = self.account_id
        token = self.token

        headers = {"Authorization": f"Bearer {token}"}

        logging.debug(
            f"Calling the API endpoint https://{url}/api/{version}/accounts/{account}/{endpoint}"
        )
        response = requests.get(
            f"https://{url}/api/{version}/accounts/{account}/{endpoint}",
            headers=headers,
        )

        # logging.debug(response.text) # for debuggin purposes
        return response.json()


    def get_jobs(self) -> List[DbtJob]:
        """Returns a list of DbtJob objects"""
        
        dbt_cloud_job_json = self.get_dbt_cloud_api(endpoint="jobs")
        dbt_cloud_jobs = DbtJobAnswer.parse_obj(dbt_cloud_job_json)

        return dbt_cloud_jobs.data


    def replicate_job_to_dbt_cloud(self, job: DbtJob) -> bool:
        """Creates or updates a job in dbt Cloud depending whether it has an id defined or not"""

        payload = _job_to_dict(job)

        cloud_url = self.url
        account = self.account_id
        headers = {"Authorization": f"Bearer {self.token}"}

        # if the job already exists the endpoint is different
        if job.id:
            url = (
                f"https://{cloud_url}/api/v2/accounts/{account}/jobs/{job.id}"
            )
        else:
            url = f"https://{cloud_url}/api/v2/accounts/{account}/jobs"
            #  we need to remove this key to avoid an error 500
            if "is_deferrable" in payload:
                del payload["is_deferrable"]

        logging.debug(f"Replicating dest job {job.id}")
        response = requests.request(
            "POST", url, headers=headers, json=payload, timeout=15
        )
        if str(response.status_code)[0] == "2":
            logging.info(
                f"✅ Job {job.id} replicated from Pydantic"
            )
            return True
        else:
            raise (
                Exception(
                    f"❌ Could not replicate the job {job.id}: {response.text}"
                )
            )

    def delete_job_in_dbt_cloud(self, job: DbtJob):

        cloud_url = self.url
        account = self.account_id
        headers = {"Authorization": f"Bearer {self.token}"}

        # to delete the job, we just need the job id
        url = f"https://{cloud_url}/api/v2/accounts/{account}/jobs/{job.id}"

        logging.debug(f"Deleting dest job {job.id}")
        response = requests.request(
            "DELETE", url, headers=headers, timeout=15
        )
        if str(response.status_code)[0] == "2":
            logging.info(
                f"✅ Dest Job {job.id} deleted in dbt Cloud"
            )
            return True
        else:
            raise (
                Exception(
                    f"❌ Could not delete the job {job.id}: {response.text}"
                )
            )