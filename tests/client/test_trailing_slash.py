from unittest.mock import MagicMock

import pytest

from dbt_jobs_as_code.client import DBTCloud, DBTCloudException


@pytest.fixture
def job_api_response():
    """Minimal valid job response from dbt Cloud API."""
    return {
        "data": {
            "id": 123,
            "account_id": 1,
            "project_id": 1,
            "environment_id": 1,
            "name": "Test Job",
            "execute_steps": ["dbt run"],
            "settings": {"threads": 4, "target_name": "prod"},
            "triggers": {"github_webhook": False, "schedule": True},
            "schedule": {"cron": "0 0 * * *"},
            "state": 1,
            "generate_docs": False,
            "run_generate_sources": False,
        }
    }


@pytest.fixture
def jobs_list_api_response(job_api_response):
    """Minimal valid jobs list response from dbt Cloud API."""
    return {
        "data": [job_api_response["data"]],
        "extra": {
            "filters": {"limit": 100, "offset": 0},
            "pagination": {"total_count": 1},
        },
    }


@pytest.mark.parametrize(
    "base_url",
    [
        "https://cloud.getdbt.com/",
        "https://custom.us1.dbt.com/",
        "https://cloud.getdbt.com///",
    ],
)
class TestTrailingSlashInBaseUrl:
    def test_get_job_url_has_no_double_slash(self, base_url, job_api_response):
        client = DBTCloud(account_id=1, api_key="test", base_url=base_url)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = job_api_response
        client._session.get = MagicMock(return_value=mock_response)

        client.get_job(job_id=456)

        called_url = client._session.get.call_args[1].get(
            "url",
            client._session.get.call_args[0][0] if client._session.get.call_args[0] else None,
        )
        assert "//" not in called_url.split("://")[1], f"Double slash in URL: {called_url}"

    def test_get_jobs_url_has_no_double_slash(self, base_url, jobs_list_api_response):
        client = DBTCloud(account_id=1, api_key="test", base_url=base_url)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = jobs_list_api_response
        client._session.get = MagicMock(return_value=mock_response)

        client.get_jobs(project_ids=[1])

        called_url = client._session.get.call_args[1].get(
            "url",
            client._session.get.call_args[0][0] if client._session.get.call_args[0] else None,
        )
        assert "//" not in called_url.split("://")[1], f"Double slash in URL: {called_url}"

    def test_update_job_url_has_no_double_slash(self, base_url, job_api_response):
        client = DBTCloud(account_id=1, api_key="test", base_url=base_url)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = job_api_response
        client._session.post = MagicMock(return_value=mock_response)

        from dbt_jobs_as_code.schemas.job import JobDefinition

        job = JobDefinition(**job_api_response["data"])
        job.identifier = "test_job"
        client.update_job(job=job)

        called_url = client._session.post.call_args[1].get(
            "url",
            client._session.post.call_args[0][0] if client._session.post.call_args[0] else None,
        )
        assert "//" not in called_url.split("://")[1], f"Double slash in URL: {called_url}"

    def test_delete_job_url_has_no_double_slash(self, base_url, job_api_response):
        client = DBTCloud(account_id=1, api_key="test", base_url=base_url)
        mock_response = MagicMock()
        mock_response.status_code = 200
        client._session.delete = MagicMock(return_value=mock_response)

        from dbt_jobs_as_code.schemas.job import JobDefinition

        job = JobDefinition(**job_api_response["data"])
        client.delete_job(job=job)

        called_url = client._session.delete.call_args[1].get(
            "url",
            client._session.delete.call_args[0][0]
            if client._session.delete.call_args[0]
            else None,
        )
        assert "//" not in called_url.split("://")[1], f"Double slash in URL: {called_url}"

    def test_create_job_url_has_no_double_slash(self, base_url, job_api_response):
        client = DBTCloud(account_id=1, api_key="test", base_url=base_url)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = job_api_response
        client._session.post = MagicMock(return_value=mock_response)

        from dbt_jobs_as_code.schemas.job import JobDefinition

        job = JobDefinition(**job_api_response["data"])
        job.identifier = "test_job"
        client.create_job(job=job)

        called_url = client._session.post.call_args[1].get(
            "url",
            client._session.post.call_args[0][0] if client._session.post.call_args[0] else None,
        )
        assert "//" not in called_url.split("://")[1], f"Double slash in URL: {called_url}"


class TestGetJobsErrorHandling:
    """get_jobs silently swallows non-401 errors, returning an empty list.

    If a malformed URL (e.g. double-slash from trailing slash in base_url)
    causes the API to return 404, the tool silently proceeds with zero cloud
    jobs, treating every YAML job as new (CREATE instead of UPDATE).
    """

    @pytest.mark.parametrize("status_code", [401, 403, 404, 500, 502])
    def test_get_jobs_raises_on_api_error(self, status_code):
        client = DBTCloud(account_id=1, api_key="test", base_url="https://cloud.getdbt.com")
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.json.return_value = {
            "status": {"code": status_code, "user_message": "error"}
        }
        client._session.get = MagicMock(return_value=mock_response)

        with pytest.raises(DBTCloudException):
            client.get_jobs(project_ids=[1])
