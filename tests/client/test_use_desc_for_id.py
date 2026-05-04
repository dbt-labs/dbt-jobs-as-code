from dbt_jobs_as_code.client import DBTCloud


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
