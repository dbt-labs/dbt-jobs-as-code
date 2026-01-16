import pytest

from dbt_jobs_as_code.schemas.job import (
    IdentifierInfo,
    JobDefinition,
    filter_jobs_by_import_filter,
)


@pytest.fixture
def test_job_factory():
    """Fixture providing a factory function to create test jobs."""

    def _create_job(name: str, filter_import: str = "") -> JobDefinition:
        # If filter_import is provided, add it to the name in the correct format
        job_name = name
        if filter_import:
            if ":" in filter_import:
                job_name = f"{name} [[{filter_import}]]"
            else:
                job_name = f"{name} [[{filter_import}:test-id]]"

        return JobDefinition(
            name=job_name,
            account_id=1,
            project_id=1,
            environment_id=1,
            settings={},
            schedule={"cron": "0 0 * * *"},
            triggers={},
            execute_steps=[],
            run_generate_sources=False,
            generate_docs=False,
        )

    return _create_job


@pytest.fixture
def mixed_filter_jobs(test_job_factory):
    """Fixture providing a list of jobs with different filter configurations."""
    return [
        test_job_factory("Job 1", "prod"),  # will become [[prod:test-id]]
        test_job_factory("Job 2", "dev"),  # will become [[dev:test-id]]
        test_job_factory("Job 3", "prod,staging:test-id"),  # will become [[prod,staging:test-id]]
        test_job_factory("Job 4"),  # empty filter
        test_job_factory("Job 5", "*"),  # will become [[*:test-id]]
    ]


class TestIdentifierExtraction:
    """Tests for the _extract_identifier_from_name function."""

    def test_no_identifier(self):
        """Test when job name has no identifier."""
        name = "My Job"
        result = JobDefinition._extract_identifier_from_name(name)
        assert result == IdentifierInfo(identifier=None, import_filter="", raw_identifier="")

    def test_simple_identifier(self):
        """Test when job name has a simple identifier."""
        name = "My Job [[test-job]]"
        result = JobDefinition._extract_identifier_from_name(name)
        assert result == IdentifierInfo(
            identifier="test-job", import_filter="", raw_identifier="test-job"
        )

    def test_identifier_with_filter(self):
        """Test when job name has an identifier with import filter."""
        name = "My Job [[prod:test-job]]"
        result = JobDefinition._extract_identifier_from_name(name)
        assert result == IdentifierInfo(
            identifier="test-job", import_filter="prod", raw_identifier="prod:test-job"
        )

    def test_identifier_with_wildcard_filter(self):
        """Test when job name has an identifier with wildcard filter."""
        name = "My Job [[*:test-job]]"
        result = JobDefinition._extract_identifier_from_name(name)
        assert result == IdentifierInfo(
            identifier="test-job", import_filter="*", raw_identifier="*:test-job"
        )

    def test_complex_identifier(self):
        """Test when job name has a complex identifier with allowed special characters."""
        name = "My Job [[env:my-complex_job-123]]"
        result = JobDefinition._extract_identifier_from_name(name)
        assert result == IdentifierInfo(
            identifier="my-complex_job-123",
            import_filter="env",
            raw_identifier="env:my-complex_job-123",
        )

    def test_invalid_format(self):
        """Test when job name has an invalid identifier format with multiple colons."""
        name = "My Job [[env:test:invalid]]"
        with pytest.raises(ValueError) as exc_info:
            JobDefinition._extract_identifier_from_name(name)
        assert (
            str(exc_info.value) == "Invalid job identifier - More than 1 colon: 'env:test:invalid'"
        )

    def test_empty_identifier(self):
        """Test when job name has empty identifier brackets."""
        name = "My Job [[]]"
        result = JobDefinition._extract_identifier_from_name(name)
        assert result == IdentifierInfo(identifier=None, import_filter="", raw_identifier="")


class TestJobFiltering:
    """Tests for the filter_jobs_by_import_filter function."""

    def test_no_filter(self, mixed_filter_jobs):
        """Test when no filter is provided, all jobs should be returned."""
        result = filter_jobs_by_import_filter(mixed_filter_jobs, None)
        assert len(result) == 5
        assert all(job in result for job in mixed_filter_jobs)

        result = filter_jobs_by_import_filter(mixed_filter_jobs, "")
        assert len(result) == 5
        assert all(job in result for job in mixed_filter_jobs)

    def test_with_filter(self, mixed_filter_jobs):
        """Test filtering jobs with specific filter value."""
        result = filter_jobs_by_import_filter(mixed_filter_jobs, "prod")
        assert len(result) == 4
        assert mixed_filter_jobs[0] in result  # matches 'prod'
        assert mixed_filter_jobs[2] in result  # matches 'prod,staging'
        assert mixed_filter_jobs[3] in result  # empty filter
        assert mixed_filter_jobs[4] in result  # wildcard filter
        assert mixed_filter_jobs[1] not in result  # 'dev' doesn't match

    def test_wildcard_filter(self, test_job_factory):
        """Test that jobs with wildcard filter are always included."""
        jobs = [
            test_job_factory("Job 1", "*"),
            test_job_factory("Job 2", "prod"),
        ]

        result = filter_jobs_by_import_filter(jobs, "any-filter")
        assert len(result) == 1
        assert jobs[0] in result
        assert jobs[1] not in result

    def test_empty_filter(self, test_job_factory):
        """Test that jobs with empty filter are always included."""
        jobs = [
            test_job_factory("Job 1"),  # empty filter
            test_job_factory("Job 2", "prod"),
        ]

        result = filter_jobs_by_import_filter(jobs, "any-filter")
        assert len(result) == 1
        assert jobs[0] in result
        assert jobs[1] not in result


class TestSAOFields:
    """Tests for State-Aware Orchestration (SAO) field handling."""

    @pytest.fixture
    def base_job_data(self):
        """Base job data for creating test jobs."""
        return {
            "name": "Test Job",
            "account_id": 1,
            "project_id": 1,
            "environment_id": 1,
            "settings": {},
            "schedule": {"cron": "0 0 * * *"},
            "triggers": {"github_webhook": False, "git_provider_webhook": False, "schedule": True},
            "execute_steps": ["dbt build"],
            "run_generate_sources": False,
            "generate_docs": False,
        }

    def test_default_sao_fields(self, base_job_data):
        """Test that SAO fields have correct default values."""
        job = JobDefinition(**base_job_data)
        assert job.force_node_selection is None
        assert job.cost_optimization_features == []

    def test_explicit_sao_fields(self, base_job_data):
        """Test that SAO fields can be set explicitly."""
        base_job_data["force_node_selection"] = False
        base_job_data["cost_optimization_features"] = ["state_aware_orchestration"]
        job = JobDefinition(**base_job_data)
        assert job.force_node_selection is False
        assert job.cost_optimization_features == ["state_aware_orchestration"]

    def test_is_ci_job_github_webhook(self, base_job_data):
        """Test CI job detection via github_webhook trigger."""
        base_job_data["triggers"]["github_webhook"] = True
        job = JobDefinition(**base_job_data)
        assert job._is_ci_or_merge_job() is True

    def test_is_ci_job_git_provider_webhook(self, base_job_data):
        """Test CI job detection via git_provider_webhook trigger."""
        base_job_data["triggers"]["git_provider_webhook"] = True
        job = JobDefinition(**base_job_data)
        assert job._is_ci_or_merge_job() is True

    def test_is_merge_job_on_merge(self, base_job_data):
        """Test merge job detection via on_merge trigger."""
        base_job_data["triggers"]["on_merge"] = True
        job = JobDefinition(**base_job_data)
        assert job._is_ci_or_merge_job() is True

    def test_is_ci_job_by_type(self, base_job_data):
        """Test CI job detection via job_type."""
        base_job_data["job_type"] = "ci"
        job = JobDefinition(**base_job_data)
        assert job._is_ci_or_merge_job() is True

    def test_is_merge_job_by_type(self, base_job_data):
        """Test merge job detection via job_type."""
        base_job_data["job_type"] = "merge"
        job = JobDefinition(**base_job_data)
        assert job._is_ci_or_merge_job() is True

    def test_scheduled_job_not_ci_or_merge(self, base_job_data):
        """Test that scheduled jobs are not detected as CI/merge."""
        job = JobDefinition(**base_job_data)
        assert job._is_ci_or_merge_job() is False

    def test_to_payload_excludes_force_node_selection_for_ci_jobs(self, base_job_data):
        """Test that force_node_selection is excluded from payload for CI jobs."""
        import json

        base_job_data["triggers"]["github_webhook"] = True
        base_job_data["force_node_selection"] = True  # Set explicitly
        job = JobDefinition(**base_job_data)

        payload = json.loads(job.to_payload())
        assert "force_node_selection" not in payload

    def test_to_payload_includes_force_node_selection_for_scheduled_jobs(self, base_job_data):
        """Test that force_node_selection is included in payload for scheduled jobs."""
        import json

        base_job_data["force_node_selection"] = True
        job = JobDefinition(**base_job_data)

        payload = json.loads(job.to_payload())
        assert payload.get("force_node_selection") is True

    def test_to_payload_includes_cost_optimization_features(self, base_job_data):
        """Test that cost_optimization_features is included in payload."""
        import json

        base_job_data["cost_optimization_features"] = ["state_aware_orchestration"]
        job = JobDefinition(**base_job_data)

        payload = json.loads(job.to_payload())
        assert payload.get("cost_optimization_features") == ["state_aware_orchestration"]

    def test_to_load_format_excludes_default_sao_fields(self, base_job_data):
        """Test that default SAO field values are not included in export."""
        job = JobDefinition(**base_job_data)
        load_format = job.to_load_format()

        # Default values should be excluded for cleaner YAML
        assert "force_node_selection" not in load_format
        assert "cost_optimization_features" not in load_format

    def test_to_load_format_includes_non_default_sao_fields(self, base_job_data):
        """Test that non-default SAO field values are included in export."""
        base_job_data["force_node_selection"] = False
        base_job_data["cost_optimization_features"] = ["state_aware_orchestration"]
        job = JobDefinition(**base_job_data)
        load_format = job.to_load_format()

        assert load_format.get("force_node_selection") is False
        assert load_format.get("cost_optimization_features") == ["state_aware_orchestration"]
