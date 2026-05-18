import json
from unittest.mock import MagicMock

from dbt_jobs_as_code.client import DBTCloud
from dbt_jobs_as_code.schemas.job import JobDefinition


class TestPreProcessJobData:
    """Tests for DBTCloud._pre_process_job_data."""

    def _make_client(self):
        return DBTCloud(
            account_id=1,
            api_key="test-key",
            use_desc_for_id=True,
        )

    def test_pre_process_extracts_identifier_from_description(self):
        """Extracts identifier from description and sets it as the job identifier."""
        client = self._make_client()
        data = {"name": "Daily Job", "description": "Runs nightly [[daily_job]]"}
        result = client._pre_process_job_data(data)
        assert result["name"] == "Daily Job [[daily_job]]"
        assert result["description"] == "Runs nightly"

    def test_pre_process_strips_identifier_from_description_empty(self):
        """When description is only the tag, result is empty string."""
        client = self._make_client()
        data = {"name": "Daily Job", "description": "[[daily_job]]"}
        result = client._pre_process_job_data(data)
        assert result["name"] == "Daily Job [[daily_job]]"
        assert result["description"] == ""

    def test_pre_process_no_identifier_in_description(self):
        """When description has no identifier, data is returned unchanged."""
        client = self._make_client()
        data = {"name": "Daily Job", "description": "No identifier here"}
        result = client._pre_process_job_data(data)
        assert result["name"] == "Daily Job"
        assert result["description"] == "No identifier here"

    def test_pre_process_no_description_field(self):
        """When description key is missing, data is returned unchanged."""
        client = self._make_client()
        data = {"name": "Daily Job"}
        result = client._pre_process_job_data(data)
        assert result == {"name": "Daily Job"}

    def test_pre_process_with_filter_in_identifier(self):
        """Handles [[filter:id]] format correctly."""
        client = self._make_client()
        data = {"name": "Daily Job", "description": "Runs nightly [[prod:daily_job]]"}
        result = client._pre_process_job_data(data)
        assert result["name"] == "Daily Job [[prod:daily_job]]"
        assert result["description"] == "Runs nightly"

    def test_pre_process_does_not_mutate_caller_dict(self):
        """_pre_process_job_data must not mutate the original dict."""
        client = self._make_client()
        original = {"name": "Daily Job", "description": "Runs nightly [[daily_job]]"}
        original_description = original["description"]
        original_name = original["name"]
        client._pre_process_job_data(original)
        assert original["description"] == original_description
        assert original["name"] == original_name

    def test_client_stores_use_desc_for_id_flag(self):
        """DBTCloud stores use_desc_for_id on the instance."""
        client = DBTCloud(account_id=1, api_key="test-key", use_desc_for_id=True)
        assert client._use_desc_for_id is True

    def test_client_defaults_use_desc_for_id_to_false(self):
        """use_desc_for_id defaults to False."""
        client = DBTCloud(account_id=1, api_key="test-key")
        assert client._use_desc_for_id is False


class TestGetJobsDescMode:
    """Integration tests: get_job/get_jobs call _pre_process_job_data when use_desc_for_id=True."""

    def _make_client(self, use_desc_for_id=True):
        return DBTCloud(
            account_id=1,
            api_key="test-key",
            use_desc_for_id=use_desc_for_id,
        )

    def _raw_job(self, name="Daily Job", description="Runs nightly [[daily_job]]"):
        """Minimal API response dict for a job."""
        return {
            "id": 42,
            "name": name,
            "description": description,
            "account_id": 1,
            "project_id": 100,
            "environment_id": 200,
            "settings": {},
            "triggers": {},
            "execute_steps": ["dbt build"],
            "run_generate_sources": False,
            "generate_docs": False,
            "schedule": {"cron": "0 0 * * *"},
            "state": 1,
        }

    def test_get_job_extracts_identifier_from_description(self):
        """get_job pre-processes API response to move [[id]] from description to name."""
        client = self._make_client(use_desc_for_id=True)
        raw = self._raw_job()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": raw}
        client._session.get = MagicMock(return_value=mock_resp)

        job = client.get_job(job_id=42)

        assert job.identifier == "daily_job"
        assert job.name == "Daily Job"
        assert job.description == "Runs nightly"

    def test_get_job_no_preprocessing_when_flag_off(self):
        """get_job does NOT pre-process when use_desc_for_id=False; identifier stays in description."""
        client = self._make_client(use_desc_for_id=False)
        # Raw API form: identifier is in description, not in name
        raw = self._raw_job(name="Daily Job", description="Runs nightly [[daily_job]]")

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": raw}
        client._session.get = MagicMock(return_value=mock_resp)

        job = client.get_job(job_id=42)

        # Without preprocessing, name has no [[id]], so identifier is None
        assert job.identifier is None
        assert job.name == "Daily Job"
        # Description is untouched — still contains the tag
        assert job.description == "Runs nightly [[daily_job]]"

    def test_get_jobs_extracts_identifiers_from_descriptions(self):
        """get_jobs pre-processes all jobs in the API response."""
        client = self._make_client(use_desc_for_id=True)
        raw_jobs = [
            self._raw_job(name="Job A", description="Desc A [[job_a]]"),
            self._raw_job(name="Job B", description="Desc B [[job_b]]"),
        ]

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": raw_jobs,
            "extra": {
                "filters": {"limit": 100, "offset": 0},
                "pagination": {"total_count": 2},
            },
        }
        client._session.get = MagicMock(return_value=mock_resp)

        jobs = client.get_jobs(project_ids=[100])

        assert len(jobs) == 2
        jobs_by_id = {j.identifier: j for j in jobs}
        assert jobs_by_id["job_a"].identifier == "job_a"
        assert jobs_by_id["job_a"].description == "Desc A"
        assert jobs_by_id["job_b"].identifier == "job_b"
        assert jobs_by_id["job_b"].description == "Desc B"

    def test_get_jobs_no_preprocessing_when_flag_off(self):
        """get_jobs does NOT pre-process when use_desc_for_id=False."""
        client = self._make_client(use_desc_for_id=False)
        raw_jobs = [
            self._raw_job(name="Job A", description="Desc A [[job_a]]"),
        ]

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": raw_jobs,
            "extra": {
                "filters": {"limit": 100, "offset": 0},
                "pagination": {"total_count": 1},
            },
        }
        client._session.get = MagicMock(return_value=mock_resp)

        jobs = client.get_jobs(project_ids=[100])

        assert len(jobs) == 1
        job = jobs[0]
        # Without preprocessing, identifier is not extracted
        assert job.identifier is None
        assert job.name == "Job A"
        assert job.description == "Desc A [[job_a]]"


class TestUpdateCreateDescMode:
    """Tests that update_job/create_job pass use_desc_for_id to to_payload."""

    def _make_job(self, identifier="daily_job", description="Runs nightly"):
        return JobDefinition(
            id=42,
            name=f"Daily Job [[{identifier}]]",
            description=description,
            account_id=1,
            project_id=100,
            environment_id=200,
            settings={},
            triggers={},
            execute_steps=["dbt build"],
            run_generate_sources=False,
            generate_docs=False,
            schedule={"cron": "0 0 * * *"},
        )

    def _make_mock_response(self, job: JobDefinition, use_desc_for_id: bool = False):
        """Build a MagicMock response that looks like a successful API response."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        raw = json.loads(job.to_payload(use_desc_for_id=use_desc_for_id))
        raw["id"] = job.id
        raw["state"] = 1
        mock_resp.json.return_value = {"data": raw}
        return mock_resp

    def test_update_job_uses_desc_for_id_when_flag_on(self):
        """update_job sends [[id]] in description when use_desc_for_id=True."""
        client = DBTCloud(account_id=1, api_key="test-key", use_desc_for_id=True)
        job = self._make_job()

        captured = {}

        def capture_post(**kwargs):
            captured["data"] = kwargs.get("data") or kwargs.get("json")
            return self._make_mock_response(job, use_desc_for_id=True)

        client._session.post = capture_post
        client.update_job(job)

        payload = json.loads(captured["data"])
        assert "[[daily_job]]" in payload["description"]
        assert "[[daily_job]]" not in payload["name"]

    def test_create_job_uses_desc_for_id_when_flag_on(self):
        """create_job sends [[id]] in description when use_desc_for_id=True."""
        client = DBTCloud(account_id=1, api_key="test-key", use_desc_for_id=True)
        job = self._make_job()

        captured = {}

        def capture_post(**kwargs):
            captured["data"] = kwargs.get("data") or kwargs.get("json")
            return self._make_mock_response(job, use_desc_for_id=True)

        client._session.post = capture_post
        client.create_job(job)

        payload = json.loads(captured["data"])
        assert "[[daily_job]]" in payload["description"]
        assert "[[daily_job]]" not in payload["name"]

    def test_update_job_uses_name_for_id_when_flag_off(self):
        """update_job sends [[id]] in name when use_desc_for_id=False (default)."""
        client = DBTCloud(account_id=1, api_key="test-key", use_desc_for_id=False)
        job = self._make_job()

        captured = {}

        def capture_post(**kwargs):
            captured["data"] = kwargs.get("data") or kwargs.get("json")
            return self._make_mock_response(job, use_desc_for_id=False)

        client._session.post = capture_post
        client.update_job(job)

        payload = json.loads(captured["data"])
        assert "[[daily_job]]" in payload["name"]
        assert "[[daily_job]]" not in payload["description"]

    def test_create_job_uses_name_for_id_when_flag_off(self):
        """create_job sends [[id]] in name when use_desc_for_id=False (default)."""
        client = DBTCloud(account_id=1, api_key="test-key", use_desc_for_id=False)
        job = self._make_job()

        captured = {}

        def capture_post(**kwargs):
            captured["data"] = kwargs.get("data") or kwargs.get("json")
            return self._make_mock_response(job, use_desc_for_id=False)

        client._session.post = capture_post
        client.create_job(job)

        payload = json.loads(captured["data"])
        assert "[[daily_job]]" in payload["name"]
        assert "[[daily_job]]" not in payload["description"]

    def test_update_job_return_value_has_identifier_in_desc_mode(self):
        """update_job pre-processes the API response so the returned JobDefinition has a clean identifier and description."""
        client = DBTCloud(account_id=1, api_key="test-key", use_desc_for_id=True)
        job = self._make_job(description="Runs nightly")

        def mock_post(**kwargs):
            return self._make_mock_response(job, use_desc_for_id=True)

        client._session.post = mock_post
        result = client.update_job(job)

        assert result.identifier == "daily_job"
        assert result.name == "Daily Job"
        assert result.description == "Runs nightly"

    def test_create_job_return_value_has_identifier_in_desc_mode(self):
        """create_job pre-processes the API response so the returned JobDefinition has a clean identifier and description."""
        client = DBTCloud(account_id=1, api_key="test-key", use_desc_for_id=True)
        job = self._make_job(description="Runs nightly")

        def mock_post(**kwargs):
            return self._make_mock_response(job, use_desc_for_id=True)

        client._session.post = mock_post
        result = client.create_job(job)

        assert result.identifier == "daily_job"
        assert result.name == "Daily Job"
        assert result.description == "Runs nightly"
